# Decisiones, supuestos y estrategias

Cubre el criterio de **Profundidad** de la consigna. Se lee en este orden:

| Parte | Qué hay |
|---|---|
| [A. Supuestos](#a-supuestos) | Lo que dimos por sentado, y con qué se rompe si es falso |
| [B. Estrategias de diseño](#b-estrategias-de-diseño) | Las decisiones arquitectónicas que atraviesan todo |
| [C. Estrategias técnicas](#c-estrategias-técnicas) | Cómo se implementan en Spark y Cassandra |
| [D. Decisiones](#d-decisiones) | Las 30 decisiones puntuales, una por una, con el número que las respalda |
| [E. Lo que nos equivocamos](#e-lo-que-nos-equivocamos) | Los errores que corregimos entre entregas |
| [F. Lo que no hicimos, a propósito](#f-lo-que-no-hicimos-a-propósito) | Y por qué |
| [G. Riesgos conocidos](#g-riesgos-conocidos) | Lo que sabemos que puede fallar |

---

## A. Supuestos

Cada uno con lo que pasa si resulta falso. Los ordenamos por cuánto dolería.

### A1 · El `exchange_rate_to_usd` de los invoices en USD es ruido, no dato
**Por qué lo asumimos.** Un tipo de cambio USD→USD distinto de 1 no existe. Los 160 invoices en USD traen valores de 0,855 a 1,118 con media 0,9977: distribución centrada en 1 con dispersión, es decir, ruido inyectado. EUR (0,998–1,198) y ARS (0,00133–0,00162) sí caen en rangos de cotización real.

**Si nos equivocamos** — si esos números fueran, por ejemplo, un ajuste contable y no ruido, el revenue de Q4 queda mal por org-mes: 2,17% mediano, 17,01% en el peor caso. El total global casi no se mueve (+0,07%), así que el error sería invisible mirando el agregado.

Es el supuesto más caro de la entrega, y el único donde una lectura distinta del dato cambia un número que se sirve.

### A2 · Los `subtotal` negativos son notas de crédito
13 de 240, el mayor `inv_jwe46xaa` = −1.671,83. Los dejamos restar del revenue porque es lo que hace una nota de crédito. La alternativa sería tratarlos como error de carga y excluirlos, que daría un revenue más alto, y probablemente incorrecto. Los marcamos con `es_nota_credito` y los contamos en `notas_credito` para que la decisión sea visible en el mart en lugar de quedar enterrada en el neto.

### A3 · `genai_tokens` nulo en v1 significa "no medido", no "cero tokens"
El campo aparece recién en `schema_version=2` (2025-07-18). En Gold lo llevamos a 0 para poder sumar, pero conceptualmente es un hueco de instrumentación, no consumo cero. Por eso Q5 muestra días de julio con 0 tokens y costo > 0 en lugar de esconderlos: el costo sí se midió, los tokens no.

### A4 · El NPS de este dataset usa la escala −100 a +100
Es la escala estándar de NPS agregado. Bajo ese supuesto los 16 valores negativos de `customers_orgs` y los 3 de `nps_surveys` son legítimos, y el único valor inválido es `101` (`org_pac56t4u`).

Si la escala fuera 0–10 por respuesta individual, entonces `nps_surveys` (que va de −16 a 68, media 26,2) estaría casi todo fuera de rango, lo cual es absurdo, así que el supuesto se sostiene solo. No afecta a ningún mart de todas formas.

### A5 · "Hoy" es el máximo del dato, no el reloj
Q2 pide "últimos 14 días" y Q3 "últimos 30". El dataset termina el 2025-08-31 (eventos) y el 2025-08-31 (tickets), así que si usáramos `now()` las ventanas darían vacías. Derivamos el corte de `max(fecha)` del propio mart. Efecto lateral bueno: la corrida es reproducible, da lo mismo cuándo se ejecute.

### A6 · Los maestros son un snapshot único
No hay columna de vigencia ni versiones por fila, así que no hay historial que versionar y **no aplica SCD Type 2**. Si los maestros llegaran como serie temporal habría que rehacer Bronze/Silver de las dimensiones, no sólo agregar una tabla.

### A7 · El volumen se mantiene en el orden de magnitud actual
43.200 eventos y 240 invoices entran cómodos en Colab. La lógica no depende del tamaño: las mismas transformaciones corren en un cluster cambiando `master()` y las rutas — pero sí lo dan por sentado dos cosas: `spark.sql.shuffle.partitions=8` (con más dato habría que subirlo) y el broadcast de las dimensiones (80 orgs; si `customers_orgs` creciera a millones, el broadcast dejaría de convenir).

### A8 · Integridad referencial limpia
Esto lo verificamos en lugar de asumirlo: 0 huérfanos. Los 80 `org_id` de los eventos existen en `customers_orgs`, y lo mismo users, tickets y billing. Por eso los joins de enriquecimiento son `left` por prolijidad, pero hoy no descartan ni dejan nulos.

---

---

## B. Estrategias de diseño

### B1 · Lambda, y por qué no Kappa
Los datos tienen dos naturalezas distintas y las tratamos por separado. Los CSV son **estados** con cadencia humana (facturación mensual, altas de clientes); `usage_events_stream` son **eventos** con cadencia de máquina. Kappa nos obligaría a inventar captura de cambios sobre CSV estáticos y a meter un broker replayable tipo Kafka, que no está en el stack de la materia.

### B2 · Medallion: cada zona resuelve un problema
Landing inmutable (poder volver al crudo) → Bronze tipado y con procedencia (`ingest_ts`, `source_file`) → Silver limpio y conformado → Gold agregado por consulta. La ventaja concreta cuando algo da raro es poder caminar hacia atrás zona por zona hasta encontrar dónde se rompió. Nos pasó con el watermark (§E1) y así lo encontramos.

### B3 · Query-first en Cassandra
Cassandra no hace joins ni consultas ad-hoc: es rápida sólo si la consulta entra por la clave que se diseñó. Entonces se arranca de la pregunta, no del modelo. Una tabla por consulta, duplicando dato deliberadamente. Siete tablas para cinco consultas (una es índice auxiliar, otra es la speed layer).

El criterio para la partition key fue siempre el mismo: **lo que va con `=` en el `WHERE`**. Y clustering `DESC` en todas las de fecha, porque las consultas miran lo reciente primero y Cassandra lee secuencial dentro de la partición.

### B4 · Colecciones donde el `groupBy` destruye información
Tres columnas de tipo colección, y el criterio no fue "usar colecciones porque suman", sino que en los tres casos agrupar perdía algo que alguien va a querer ver.

- `severities map<text,int>` — al agrupar tickets por (org, día) se pierde el detalle por severidad. 4 claves posibles, colección acotada.
- `currencies set<text>` — al normalizar a USD se pierde en qué moneda facturó.
- `anomaly_methods set<text>` — qué métodos coincidieron, que es lo que permite auditar por qué algo se marcó.

### B5 · Marcar, no borrar
Las anomalías se marcan y siguen; los costos no se recortan. Un pico de costo es justo lo que FinOps quiere ver, no algo a suavizar. Lo mismo con las notas de crédito y con los días de 0 tokens: el pipeline hace visible lo raro en vez de normalizarlo.

La excepción son las tres reglas de calidad, que sí sacan filas, pero a **quarantine**, no a la basura: quedan en Parquet completo, se pueden inspeccionar y se pueden re-procesar.

### B6 · Los parámetros salen del dato
Ventanas, orgs de ejemplo y rangos de fecha se derivan del mart en tiempo de ejecución (`max(usage_date)`, la org de mayor costo, etc.). Nada hardcodeado. Es lo que hace que el notebook dé el mismo resultado corrido hoy o en tres meses.

---

---

## C. Estrategias técnicas

### C1 · Esquema explícito, nunca `inferSchema`
Con este dataset `inferSchema` rompe: `value` viene a veces como número y a veces como string `"100.0"`. Spark inferiría double y anularía las filas string al parsear. Lo declaramos **string** y casteamos a `value_num` con fallback a null, conservando el crudo para la evidencia de quarantine.

Un solo esquema para v1 y v2, con `carbon_kg` y `genai_tokens` **nullable**. Sin ramas `if version == 2`: v1 los deja en null, v2 los llena. La evolución de esquema se maneja con tipos, no con código condicional.

### C2 · Carga distribuida con `foreachPartition`
Cada partición de Spark abre su sesión contra Astra, prepara el statement una vez y manda los INSERT con `execute_async` en ventanas de 500 futures (pipeline sin inflar memoria; el `.result()` propaga el error si alguno falla), y cierra el cluster en un `finally`.

**El mart nunca pasa por el driver.** Traerlo con `collect()` o `toLocalIterator()` convierte al driver en cuello de botella y limita la carga a lo que entre en su memoria. Es el antipatrón que la corrección de la primera entrega nos marcó, y esta versión lo elimina. La speed layer usa la misma función adentro del `foreachBatch`, porque un micro-batch es un DataFrame batch común.

### C3 · Broadcast en los joins de enriquecimiento
`customers_orgs` tiene 80 filas y la tabla de estadísticos por servicio tiene 6. Broadcastearlas manda la tabla chica a cada executor en vez de mover los eventos por la red, sin shuffle. Lo verificamos en el plan físico: aparece `BroadcastHashJoin` y no `Exchange` del lado de los eventos ([`evidence/explain_broadcast.txt`](evidence/explain_broadcast.txt)).

### C4 · Zona horaria de la sesión fijada en UTC
`usage_date` sale de `to_date(event_ts)`, y `to_date` convierte usando la zona horaria de la **sesión de Spark**, que por defecto hereda la del sistema operativo. Los eventos vienen con timestamp ISO-8601 en UTC, así que la agregación diaria tiene que ser en UTC o el día no significa lo mismo.

No es un detalle cosmético: corriendo la misma corrida en UTC−3, todo evento entre las 00:00 y las 03:00 UTC cae en el día anterior.

| TZ de sesión | grupos (org, servicio, fecha) | fechas distintas |
|---|---|---|
| UTC | 11.050 | 60 |
| UTC−3 | 12.157 | 61 |

El dataset cubre del 2025-07-03 al 2025-08-31, o sea 60 días exactos: las 61 fechas del segundo caso incluyen un `2025-07-02` que no existe en el origen. Por eso fijamos `spark.sql.session.timeZone=UTC` al crear la sesión, y el mart da lo mismo corra donde corra. La configuración de sesión es parte de la lógica, no del entorno.

### C5 · Particionado según quién filtra
- Eventos y Silver: `usage_date` + `service` — es por donde filtran las consultas.
- Gold FinOps: `usage_date`.
- Tickets en Silver: `severity`, **no** fecha. Son 1000 filas en 115 fechas distintas: partir por fecha deja 115 parquets de ~8 filas, que es el antipatrón de archivos chicos.
- Maestros: columnas de baja cardinalidad (`hq_region`, `role`, `currency`, `category`). Honestamente acá es más por el requisito que por una consulta concreta.

### C6 · Anomalías por consenso
z-score, MAD y p99, calculados **por servicio** porque los costos no están en la misma escala. Se marca sólo cuando coinciden **≥2 de 3**: baja de 211 a 89 marcas. Cada método tiene su punto ciego: el z-score se distorsiona con la misma cola que busca, p99 es bruto por definición, y MAD aguanta pero es conservador. El consenso corta falsos positivos. `K=1.5` sobre p99 es el único número que elegimos a mano; 3σ y el 1,4826 del MAD son estándar.

### C7 · Idempotencia por tres vías
Archivos replayables + checkpoint en el stream + UPSERT por primary key. Las tres juntas dan exactly-once práctico. Si la corrida se cae a la mitad, el checkpoint sabe qué archivos ya consumió, y lo que se haya escrito a Cassandra se pisa con el mismo valor. Se prueba con conteos antes/después sobre las 6 tablas.

Aclaración honesta sobre la dedupe: el dataset trae **43.200 `event_id` únicos sobre 43.200 eventos**, así que hoy no borra una sola fila. Está para que un re-procesamiento sea idempotente si un archivo se re-entrega, no porque haya duplicados.

### C8 · Las evidencias las escribe el pipeline
La celda de reporte escribe `docs/evidence/*.txt` además de imprimir. Los archivos que lee el corrector salen de la misma corrida que produjo los números, sin copiar y pegar, que es como se cuelan las inconsistencias (nos pasó: ver §E2).

---

---

## D. Decisiones

Las decisiones puntuales, numeradas. Las partes B y C explican el criterio general; acá está el caso por caso.

### D1 · Lambda vs Kappa
Elegimos **Lambda**. Los CSV (orgs, users, billing, etc.) son estados que cambian con cadencia humana → batch. `usage_events_stream` viene fragmentado para simular un feed → streaming. Kappa nos obligaría a inventar captura de cambios sobre CSV estáticos y a meter un broker tipo Kafka, que no está en el stack que nos dieron.

### D2 · Tablas CQL, no Document API
Usamos tablas CQL, no el Document API de Astra, para poder definir la partition key. La consigna evalúa el modelado query-first y eso sólo existe en las tablas CQL. (Ver también #19 sobre el tipo colección, que sí usamos.)

### D3 · Particionado
- **Eventos / Silver:** por `usage_date` y `service` (las consultas filtran por ahí).
- **Gold:** por `usage_date`.
- **Maestros (Bronze):** por `hq_region` / `role` / `currency` / `category`. Es para cumplir el requisito de Parquet particionado; nadie filtra los maestros por esas columnas. En producción se elegiría según las queries.
- **Tickets (Silver):** por `severity`, **no** por fecha. Son 1000 tickets repartidos en 115 fechas distintas, así que particionar por fecha deja 115 carpetas de ~8 filas. Lo medimos escribiendo las dos versiones:

  | `partitionBy` | tamaño en disco | archivos parquet |
  |---|---|---|
  | `severity` (4 valores) | 280 KB | 24 |
  | `ticket_date` (115 valores) | 4,4 MB | 558 |

  16× más disco y 23× más archivos para las mismas 1.000 filas. Cada parquet arrastra su footer y su overhead de apertura, y el planner tiene que listarlos todos antes de leer nada: es el antipatrón de archivos chicos. Además ninguna consulta filtra tickets por día sin filtrar antes por org, así que la partición por fecha no aporta pruning.
- `spark.sql.shuffle.partitions=8` porque el dato es chico (el default 200 hace tareas diminutas en Colab).

### D4 · Claves Cassandra
`PRIMARY KEY ((org_id, service), usage_date)`. Q1 pide costo diario por org+servicio en un rango → con esa clave es una lectura de una sola partición, ordenada por fecha. Una tabla por consulta.

`usage_date DESC` (no ASC) porque las consultas miran lo más reciente primero, y Cassandra lee secuencial dentro de la partición, y tener lo nuevo arriba evita recorrer toda la partición.

Tamaño de partición: 80 orgs × 6 servicios = 480 particiones posibles, ~23 filas cada una (11.050/480). Son particiones chicas, lo cual consideramos suficiente para Cassandra (el problema serían particiones gigantes).

Las otras cuatro tablas parten sólo por `org_id`: 80 particiones de 3 filas (revenue), ~12 (tickets), ~15 (genai) y ~1 (anomalías). Nada cerca del límite práctico de Cassandra.

### D5 · Features de `metric`
`requests`, `cpu_hours`, etc. salen de `sum(value WHERE metric=X)`, no de `count(*)`.

### D6 · `value` como string
`value` viene como número, como `"100.0"` o nulo. Si lo declarábamos `double`, Spark anulaba las filas string al parsear. Lo leemos como string y casteamos a `value_num`, guardando el dato crudo para la evidencia de quarantine.

### D7 · Anomalías
z-score solo marca mucho por la cola de outliers, p99 es medio bruto, MAD aguanta mejor. Por eso marcamos sólo cuando coinciden **2 de 3**. Con eso bajamos de 211 a 89. Los estadísticos van por servicio (los costos no están en la misma escala). `K=1.5` sobre p99 es el único número que elegimos a mano; el resto (3σ, 1.4826 del MAD) son estándar. Guardamos qué métodos dispararon en `anomaly_methods`.

### D8 · Watermark de 60 días (antes 10 minutos — lo teníamos mal)
En la primera entrega pusimos 10 minutos razonando sobre la granularidad del `timestamp`. Al revisar los archivos vimos que el razonamiento no aplicaba: **ninguno de los 120 JSONL es un corte temporal**. Cada uno trae eventos de los 60 días completos. `events_part_0000` va del 2025-07-03 al 2025-08-31, y el 0119 también. La fragmentación simula micro-lotes pero es un muestreo aleatorio, no cronológico.

Con esa fuente, un watermark corto rompe el pipeline. `dropDuplicates` con watermark descarta lo que cae por debajo de `max(event_time) − delay`; apenas el primer micro-lote ve un evento del 31-08, el watermark salta al final del dataset y todo julio queda "atrasado". Lo medimos forzando micro-lotes con `maxFilesPerTrigger`:

| watermark | micro-lotes | eventos en Bronze |
|---|---|---|
| 10 min | 1 (todos los archivos juntos) | 43.200 / 43.200 |
| 10 min | 12 (`maxFilesPerTrigger=10`) | **7.203 / 43.200** |
| 60 días | 12 (`maxFilesPerTrigger=10`) | 43.200 / 43.200 |

Con 10 minutos, entonces, el pipeline funcionaba **por casualidad**: `availableNow` sin `maxFilesPerTrigger` mete los 120 archivos en un solo micro-lote y ahí nada llega tarde. Partiendo la corrida en lotes se pierde el 83% de los eventos, en silencio: sin error ni warning, sólo menos filas en Gold.

Lo pusimos en 60 días, que cubre el span completo. El estado no se dispara porque la cardinalidad de la clave está acotada: son 43.200 `event_id` de un histórico cerrado. Con un feed real y llegadas cronológicas se dimensionaría al revés, midiendo la latencia real de llegada.

La celda de evidencia del notebook corre las tres combinaciones para que el número se vea, no se afirme.

### D9 · Trigger `availableNow`
La fuente es una carpeta fija de archivos. `availableNow` procesa todo y termina, así la corrida es reproducible y no deja un stream colgado. Con un feed en vivo se cambiaría a `processingTime`.

### D10 · Idempotencia
Archivos replayables + checkpoint en el stream + UPSERT por clave natural. Re-ejecutar no cambia el conteo (se prueba en el notebook con antes/después).

### D11 · Output mode
`append` en la ingesta (no agrega nada). `update` en la Speed Layer porque la agregación diaria se actualiza con cada micro-batch, y el UPSERT de Cassandra pisa por clave.

### D12 · Conformance
Miramos `service`, `region` y `metric` con un `distinct().show()` en el perfilado: vienen consistentes (`compute`, `us-east`, `requests`…), sin variantes de casing ni espacios. Por eso no agregamos `upper/trim` — no hay necesidad de transformar algo que ya está limpio. Si apareciera ruido de casing, iría en Silver.

### D13 · Nulos
Nulos de `unit` con `value` presente → quarantine (~2038). El resto (value nulo, nps nulo, csat nulo) los dejamos como NULL (no tiene sentido inventar un valor). Las features numéricas se llevan a 0 sólo en Gold, donde 0 = "sin actividad ese día".

### D14 · SCD
No aplica: los maestros son un único snapshot, no hay historial de cambios que guardar. Si en algún momento se entregaran como serie temporal, ahí sí entraría un SCD Type 2.

### D15 · Alcance de calidad
Las tres reglas de la consigna corren sobre los eventos (value string/nulo, regla 3, costos negativos/picos, dedupe, v1/v2) y alimentan la quarantine.

Con los marts de Soporte y Revenue sumamos el tratamiento de billing (FX, créditos vacíos, notas de crédito — ver #25) y de tickets (csat nulo, tickets abiertos — ver #26). Quedan documentadas y **sin tratar** las issues de archivos que ningún mart consume: 25 inconsistencias `is_enterprise`/`plan_tier`, 89 usuarios con rol `admin` no documentado, 232 con `last_login < created_at`, 47 recursos con tags en conflicto. No las limpiamos porque no entran en ninguna de las 5 consultas; limpiarlas sería trabajo que no cambia ningún resultado.

### D16 · NPS: qué está fuera de rango y qué no
Hay exactamente **un** valor fuera de rango: `nps_score=101` en `org_pac56t4u` (`customers_orgs`). NPS va de −100 a +100, así que 101 es imposible.

Lo que **no** es un problema, aunque lo parezca: los valores negativos. Hay 16 en `customers_orgs` (hasta −38) y 3 en `nps_surveys` (hasta −16), y son todos legítimos — un NPS negativo es simplemente más detractores que promotores. En la primera entrega los teníamos listados junto al 101 como si fueran el mismo tipo de suciedad; no lo son.

`nps_surveys` completo va de −16 a 68, media 26,2: nada fuera de rango ahí.

No nos afecta de todas formas: usamos orgs sólo para enriquecer con industry/plan/region/lifecycle, y `nps_score` no entra en ningún mart de las 5 consultas.

### D17 · Dedupe doble
La dedupe del stream está acotada por el watermark, así que agregamos un `dropDuplicates(["event_id"])` por lotes en Silver para unicidad global sin depender de él.

Contamos los `event_id` del landing: **43.200 únicos sobre 43.200 eventos**. O sea que hoy ninguna de las dos dedupes borra una sola fila. Las dejamos igual, pero por lo que son: lo que hace idempotente un re-procesamiento si un archivo se re-entrega, no una limpieza de duplicados que el dataset no tiene. Decirlo al revés en el video sería inventar un problema.

### D18 · Listar servicios en Q2
Q2 necesita saber qué servicios tiene una org. No lo hardcodeamos (un servicio nuevo quedaría afuera sin avisar) ni queremos escanear toda la tabla (triggerea un warning de Cassandra y no escala). Materializamos una tabla índice `services_by_org` con PK `((org_id), service)`: Q2 lee los servicios de una org en **una sola partición**, sin scan y siempre actualizada. Se puebla en la misma carga.

### D19 · Tipo colección en Cassandra → `anomaly_methods set<text>`
La tabla usa una columna de tipo colección: `anomaly_methods set<text>`. Guarda qué métodos marcaron anomalía ese día (p. ej. `{'zscore','mad'}`). Un `set` encaja porque es un conjunto sin orden ni repetidos, y el dato ya lo calculamos en Silver. Lo agregamos al `CREATE TABLE`, al `INSERT` y a la agregación de Gold (`collect_set` + `flatten`).

### D20 · Trade-off de la regla 3
La regla 3 manda a quarantine filas con `unit` nulo aunque tengan un `cost_usd_increment` válido, así que subestima el costo total en Gold. El notebook mide cuánto cuesta esa decisión (suma del cost de las filas quarantineadas con cost válido). La dejamos así porque la consigna pide esa regla, pero queda documentado. Una alternativa de producción: conservar el costo para la agregación y aislar sólo el `value` problemático.

### D21 · Speed Layer a tabla propia
La capa de velocidad (`foreachBatch`) escribe a `org_daily_usage_stream`, **no** al mart del batch. Sólo calcula costo y requests; si escribiera a la tabla del batch, pondría en cero `cpu_hours`/`storage`/`genai` y pisaría lo que cargó el batch. Tabla separada = las dos vías conviven sin destruirse.

### D22 · Performance
- Carga a Cassandra **distribuida con `foreachPartition`**. Cada partición de Spark abre su propia sesión contra Astra, prepara el statement una vez y manda los INSERT con `execute_async` en ventanas de 500 futures (pipeline sin inflar memoria; el `.result()` propaga el error si alguno falla). El mart nunca pasa por el driver. La alternativa que teníamos antes (`toLocalIterator` + `execute_concurrent_with_args`) traía todo al driver, que es el antipatrón que convierte al driver en cuello de botella: escala hasta donde entra en su memoria y no más. La Speed Layer usa la misma función adentro del `foreachBatch`, porque el micro-batch es un DataFrame batch común.
- `broadcast` en los joins (orgs ~80 filas, stats ~6) → sin shuffle. Lo confirmamos con `explain("formatted")`: en el plan aparece `BroadcastHashJoin`.
- Compactación: el Bronze de streaming con `availableNow` escribe un Parquet por micro-batch, así que quedan archivos chicos. Para el MVP **decidimos no compactar** porque en Colab el volumen no lo justifica; en producción un `OPTIMIZE`/compactación post-ingesta los junta.

### D23 · Verificar el plan con `explain`
Agregamos un `silver.explain("formatted")` después del join de enriquecimiento para confirmar que Spark usa `BroadcastHashJoin` (y no un shuffle join). Es la evidencia directa de que el `broadcast` que pedimos efectivamente se aplica.

### D24 · Merge batch + speed en serving → fuera del MVP
En una Lambda completa la capa de serving mergea el resultado del batch (histórico completo) con el de la speed (lo más reciente) al consultar. No lo implementamos: servimos las dos vías en tablas separadas (`org_daily_usage_by_service` y `org_daily_usage_stream`) y el merge sería trabajo de la capa de serving en la entrega final. Para el MVP, con fuente estática, las dos calculan lo mismo, así que el merge no aportaría nada distinto todavía.

### D25 · FX: forzamos USD → 1.0
`billing_monthly` trae `exchange_rate_to_usd` en las tres monedas. Miramos el rango por moneda:

| moneda | n | fx mín | fx máx | media |
|---|---|---|---|---|
| USD | 160 | 0,85463 | 1,11791 | 0,99769 |
| EUR | 29 | 0,99810 | 1,19808 | 1,10165 |
| ARS | 51 | 0,00133 | 0,00162 | 0,00150 |

EUR y ARS son cotizaciones plausibles. Los USD no: un tipo de cambio USD→USD distinto de 1 no existe. Son 144 de 160 invoices con `|fx − 1| > 1%`, ruido inyectado alrededor de 1 (media 0,9977).

**Decisión: forzamos 1.0 para USD y respetamos el campo para EUR y ARS.**

Lo que nos hizo dudar: en el total global el ajuste casi no se nota (164.184,90 → 164.293,06, un +0,07%), porque el ruido es simétrico y se cancela. Pero el grano de Q4 no es el total global, es `(org_id, month)`, y ahí no se cancela nada:

- desvío mediano por org-mes: **2,17%**
- desvío máximo: **17,01%** (`org_g8sbi4q2`, 2025-06)
- 56 de 240 filas se mueven más de 5%; 8 más de 10%

O sea que mirando sólo el total habríamos concluido que daba igual. El notebook imprime esa tabla comparativa como evidencia.

Los 13 subtotales negativos (el mayor, `inv_jwe46xaa`, −1.671,83) los dejamos sumar: son notas de crédito, importes que la empresa devuelve, y restarlas del revenue es lo correcto. Las marcamos con `es_nota_credito` y las contamos en `notas_credito` para que se vean en el mart en vez de esconderse dentro del neto. Los `credits` vacíos (137 de 240) van a 0 con `coalesce`.

### D26 · CSAT: no lo imputamos y no lo recortamos
254 de los 1000 tickets no tienen `csat`. Los dejamos **NULL**: `avg()` saltea los nulos solo, así que no hace falta ningún centinela. Al cargar a Cassandra `avg_csat` va NULL si esa org no tuvo ninguna respuesta ese día (229 org-días) — un 0.0 se leería en el dashboard como "csat pésimo" en lugar de "sin datos".

Imputar 0.0 es la opción cómoda y rompe el dato: hay **11 tickets con csat = 0.0 real**, que quedarían mezclados con las no-respuestas sin forma de distinguirlos después.

Tampoco recortamos por rango, y esto lo chequeamos antes de decidir. La distribución es 0:11, 1:45, 2:127, 3:242, 4:197, 5:95, 6:28, 7:1. La primera lectura fue "la escala es 1–5, entonces 0, 6 y 7 son ruido", pero no cierra. Ajustando contra una normal redondeada (μ=3,30, σ=1,26) los esperados dan 8,8 / 47,2 / 138,7 / 224,1 / 199,0 / 97,1 / 26,0 / 3,8: pega en todo el rango, **incluidas las colas**. No hay corte en 5 ni valores implantados; es una gaussiana redondeada generada sobre 0–7.

Así que no hay nada fuera de rango que limpiar. CSAT tampoco tiene una escala estándar en la industria (1–5, 1–7, 1–10 y 0–10 son todas comunes) y la consigna no define ninguna, así que cualquier regla de rango habría sido una suposición nuestra disfrazada de regla de calidad. Habríamos mandado 40 filas legítimas a quarantine.

Contraste con #16: el NPS **sí** tiene rango definido (−100 a +100), por eso ahí el 101 sí es un error.

### D27 · Marts: uno por consulta
Cinco marts en Gold, uno por consulta más el de anomalías que pide el punto 4 de la consigna:

| mart | grano | filas | consulta |
|---|---|---|---|
| `org_daily_usage_by_service` | org × servicio × día | 11.050 | Q1, Q2 |
| `tickets_by_org_date` | org × día | 944 | Q3 |
| `revenue_by_org_month` | org × mes | 240 | Q4 |
| `genai_tokens_by_org_date` | org × día (sólo genai) | 1.131 | Q5 |
| `cost_anomaly_mart` | org × servicio × día | 89 | requisito 4 |

Q5 podría servirse de `org_daily_usage_by_service` filtrando `service='genai'` (la partition key es `(org_id, service)`, así que la consulta es válida). Le hicimos tabla propia igual porque el grano de Q5 es org × día, no org × servicio × día, y con la tabla dedicada la consulta es una lectura de partición sin post-procesar.

### D28 · Colecciones CQL
Tres columnas de tipo colección, todas donde el agrupamiento perdía información real:

- `anomaly_methods set<text>` — qué métodos marcaron anomalía ese día.
- `severities map<text,int>` — conteo por severidad, que se pierde al agrupar tickets por (org, día). Son 4 claves posibles, así que la colección queda acotada.
- `currencies set<text>` — en qué monedas facturó la org ese mes, que se pierde al normalizar todo a USD.

El criterio fue ese: colección donde el `groupBy` destruía un dato que alguien va a querer ver, no para marcar la casilla de "usamos colecciones".

### D29 · Q3: por qué el grano es (org, día) y no (org, severidad, día)
La consigna pide "tickets críticos y tasa de SLA breach por día". Con PK `((org_id), ticket_date)` la evolución diaria sale de una sola partición y el conteo de críticos va como columna. Meter `severity` en la partition key partiría cada org en 4 y obligaría a 4 lecturas para reconstruir el día completo. El detalle por severidad ya está en el `map<text,int>`, así que no se pierde nada.

**Sobre el tamaño del resultado.** Q3 devuelve pocas filas por org y es propio del dataset, no un error: son 1.000 tickets repartidos en 80 orgs y 115 fechas, así que una ventana de 30 días le toca a cada org con 3 filas en promedio (mínimo 1, máximo 7, sobre 77 orgs con actividad en agosto). La org de ejemplo se elige por críticos **dentro de la ventana** y no por su máximo histórico: eligiéndola global cae una cuyo pico está en julio y la consulta de agosto devuelve una sola fila.

Muchos `avg_csat` vienen NULL por lo mismo: sólo 746 de los 1.000 tickets tienen encuesta respondida, así que hay días sin ninguna. Es la decisión de #D26, y se ve en el resultado.

### D30 · Tokens GenAI y la evolución de esquema
`genai_tokens` sólo existe en `schema_version=2` (desde 2025-07-18) y sólo para `service='genai'`. En el mart de Q5 eso se ve como días de julio con 0 tokens y costo > 0. **No lo rellenamos ni lo escondemos**: es exactamente la evolución de esquema que la consigna quiere que manejemos, y taparla con un valor inventado sería peor que mostrarla.

De los 2.558.359 tokens del landing llegan 2.418.410 a Gold. Los 139.949 que faltan están en 160 eventos genai que la regla 3 mandó a quarantine (`unit` nulo con `value` presente). Es el mismo trade-off de #20, medido: 5,5% de los tokens.

---

## E. Lo que nos equivocamos

Esta sección existe porque las dos cosas que más nos enseñaron del dataset fueron errores nuestros.

### E1 · El watermark de 10 minutos estaba mal y no lo sabíamos
Lo elegimos razonando sobre la granularidad del `timestamp` (minutos) y sobre que los archivos "simulan lotes de ~5 minutos". El razonamiento era plausible y estaba equivocado, porque nunca habíamos mirado el rango temporal de cada archivo.

Cuando lo miramos: **ninguno de los 120 archivos es un corte temporal**. Cada uno cubre los 60 días completos. `events_part_0000` va del 03-07 al 31-08, y el `0119` también. La fragmentación simula micro-lotes pero es un muestreo aleatorio.

Con esa fuente, `dropDuplicates` con watermark corto descarta casi todo: apenas el primer micro-lote ve un evento del 31-08, el watermark salta al final y todo julio queda "atrasado". Medido con `maxFilesPerTrigger=10`:

| watermark | micro-lotes | eventos en Bronze |
|---|---|---|
| 10 min | 1 (todos juntos) | 43.200 / 43.200 |
| 10 min | 12 | **7.203 / 43.200** |
| 60 días | 12 | 43.200 / 43.200 |

Es decir que funcionaba **por casualidad**: `availableNow` sin `maxFilesPerTrigger` mete los 120 archivos en un único micro-lote y ahí nada llega tarde. La pérdida es silenciosa — sin error ni warning, sólo menos filas en Gold.

Lo que aprendimos: un watermark no se elige razonando sobre lo que *debería* ser la fuente, se elige mirando la fuente. Y que un pipeline que anda no es lo mismo que un pipeline correcto.

### E2 · Las evidencias de la primera entrega no cerraban entre sí
Los archivos de evidencia se armaban a mano, pegando la salida de corridas distintas, así que nada garantizaba que los números de un archivo cerraran con los del otro. Por eso ahora los escribe el pipeline en una sola pasada (§C8): si `c1` dice 43.200 y `c2` dice 40.956 + 2.244, es porque salieron de la misma ejecución y no de dos.

Por eso ahora los escribe el pipeline (§C8).

### E3 · Casi limpiamos datos que estaban bien
Dos veces estuvimos por marcar como "sucio" algo legítimo.

- **CSAT.** La distribución (0:11, 1:45, 2:127, 3:242, 4:197, 5:95, 6:28, 7:1) parecía una escala 1–5 con ruido en 0, 6 y 7. Ajustándola contra una normal redondeada (μ=3,30, σ=1,26) los esperados dan 8,8 / 47,2 / 138,7 / 224,1 / 199,0 / 97,1 / 26,0 / 3,8, y pega en **todo** el rango, colas incluidas. Es una gaussiana sobre 0–7, no una escala recortada. Una regla de rango habría mandado 40 filas legítimas a quarantine.
- **NPS.** Teníamos los valores negativos listados junto al 101 como si fueran el mismo tipo de suciedad. No lo son: un NPS negativo es simplemente más detractores que promotores. El único valor inválido es el 101.

En los dos casos pasa lo mismo: "esto parece raro" no es lo mismo que "esto está mal", y la diferencia se resuelve mirando la distribución y no la intuición.

---

---

## F. Lo que no hicimos, a propósito

| Qué | Por qué |
|---|---|
| Mart dedicado de top-N para Q2 | El ranking iría en la clustering key, así que cada carga obliga a borrar y reescribir la partición entera de la org. Con 6 servicios por org, ordenar del lado de la app cuesta 6 lecturas de partición. Se justificaría con muchos más servicios. |
| Merge batch + speed en serving | Con fuente estática las dos vías calculan lo mismo, así que el merge no aportaría nada distinto todavía. Las servimos en tablas separadas para que no se pisen. |
| SCD Type 2 | No hay historial en los maestros (§A6). |
| Limpiar `resources`, `marketing_touches`, `nps_surveys` | Ninguna de las 5 consultas los toca. Limpiarlos sería trabajo que no cambia ningún resultado. |
| Compactar los Parquet del stream | `availableNow` escribe un archivo por micro-batch, así que quedan chicos. En Colab el volumen no lo justifica; en producción iría una compactación post-ingesta. |
| Tratar las inconsistencias de maestros | 25 casos `is_enterprise`/`plan_tier`, 89 usuarios con rol `admin` no documentado, 232 con `last_login < created_at`, 47 recursos con tags en conflicto. Documentados, sin tratar: no entran en ningún mart. |

---

---

## G. Riesgos conocidos

- **La regla 3 subestima el costo.** Manda a quarantine filas con `unit` nulo aunque el `cost_usd_increment` sea válido: 7.110,15 USD de costo válido queda fuera de Gold, y 139.949 tokens GenAI (5,5% del total). Lo dejamos porque la consigna pide esa regla, pero medido. En producción convendría conservar el costo y aislar sólo el `value`.
- **`/content` es efímero.** Parquet y checkpoints se pierden entre sesiones de Colab. Para persistir, montar Drive.
- **Un solo keyspace, sin réplica configurada.** Es lo que da Astra en serverless; el diseño de particiones no depende de eso.
- **Streaming sobre archivos, no Kafka.** Nos da replayability gratis (los archivos siguen ahí), pero no da orden ni garantías de entrega reales. Lo de §E1 es consecuencia directa de esto.
