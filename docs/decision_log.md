# Log de decisiones

## 1. Lambda vs Kappa
Elegimos **Lambda**. Los CSV (orgs, users, billing, etc.) son estados que cambian con cadencia humana → batch. `usage_events_stream` viene fragmentado para simular un feed → streaming. Kappa nos obligaría a inventar captura de cambios sobre CSV estáticos y a meter un broker tipo Kafka, que no está en el stack que nos dieron.

## 2. Tablas CQL, no Document API
Usamos tablas CQL, no el Document API de Astra, para poder definir la partition key. La consigna evalúa el modelado query-first y eso sólo existe en las tablas CQL. (Ver también #19 sobre el tipo colección, que sí usamos.)

## 3. Particionado
- **Eventos / Silver:** por `usage_date` y `service` (las consultas filtran por ahí).
- **Gold:** por `usage_date`.
- **Maestros (Bronze):** por `hq_region` / `role` / `currency`. Es para cumplir el requisito de Parquet particionado; en este MVP nadie filtra los maestros por esas columnas. En producción se elegiría según las queries.
- `spark.sql.shuffle.partitions=8` porque el dato es chico (el default 200 hace tareas diminutas en Colab).

## 4. Claves Cassandra
`PRIMARY KEY ((org_id, service), usage_date)`. Q1 pide costo diario por org+servicio en un rango → con esa clave es una lectura de una sola partición, ordenada por fecha. Una tabla por consulta.

`usage_date DESC` (no ASC) porque las consultas miran lo más reciente primero, y Cassandra lee secuencial dentro de la partición — tener lo nuevo arriba evita recorrer toda la partición.

Tamaño de partición: 80 orgs × 6 servicios = 480 particiones posibles, ~23 filas cada una (11050/480). Son particiones chiquitas, lo cual está perfecto para Cassandra (el problema serían particiones gigantes, no chicas).

## 5. Features de `metric`
`requests`, `cpu_hours`, etc. salen de `sum(value WHERE metric=X)`, no de `count(*)`. Contar filas etiquetaría "cantidad de eventos" como "requests", que es otra cosa. (Bug real que evitamos.)

## 6. `value` como string
`value` viene como número, como `"100.0"` o nulo. Si lo declarábamos `double`, Spark anulaba las filas string al parsear. Lo leemos string y casteamos a `value_num` nosotros, guardando el crudo para la evidencia de quarantine.

## 7. Anomalías
z-score solo marca mucho por la cola de outliers, p99 es medio bruto, MAD aguanta mejor. Por eso marcamos sólo cuando coinciden **2 de 3** — con eso bajamos de ~211 a ~89. Los estadísticos van por servicio (los costos no están en la misma escala). `K=1.5` sobre p99 es el único número que elegimos a mano; el resto (3σ, 1.4826 del MAD) son estándar. Guardamos qué métodos dispararon en `anomaly_methods`.

## 8. Watermark de 10 min
Elegimos 10 minutos porque la granularidad del `timestamp` es de minutos y los archivos simulan lotes de ~5 min, así que 10 da margen para que llegue un lote tarde sin descartarlo. Es un valor de demo (los JSONL son estáticos, no hay latencia real que medir); con un feed productivo se ajustaría midiendo cuánto llega tarde de verdad.

## 9. Trigger `availableNow`
La fuente es una carpeta fija de archivos. `availableNow` procesa todo y termina, así la corrida es reproducible y no deja un stream colgado. Con un feed vivo se cambiaría a `processingTime` y nada más del código cambia.

## 10. Idempotencia
Archivos replayables + checkpoint en el stream + UPSERT por clave natural. Re-ejecutar no cambia el conteo (se prueba en el notebook con antes/después).

## 11. Output mode
`append` en la ingesta (no agrega nada). `update` en la Speed Layer porque la agregación diaria se actualiza con cada micro-batch, y el UPSERT de Cassandra pisa por clave.

## 12. Conformance
Miramos `service`, `region` y `metric` con un `distinct().show()` en el perfilado: vienen consistentes (`compute`, `us-east`, `requests`…), sin variantes de casing ni espacios. Por eso no agregamos `upper/trim` — sería transformar algo que ya está limpio. Si apareciera ruido de casing, iría en Silver.

## 13. Nulos
Nulos de `unit` con `value` presente → quarantine (~2038). El resto (value nulo, nps nulo, csat nulo) los dejamos como NULL — no tiene sentido inventar un valor. Las features numéricas se llevan a 0 sólo en Gold, donde 0 = "sin actividad ese día".

## 14. SCD
No aplica: los maestros son un único snapshot, no hay historial de cambios que guardar. Si en algún momento se entregaran como serie temporal, ahí sí entraría un SCD Type 2.

## 15. Alcance de calidad
El MVP ataca lo que toca el mart FinOps (eventos + org_id): value string/nulo, regla 3, costos negativos/picos, dedupe, v1/v2. Se documentan pero no se tratan las issues de archivos fuera del MVP, que vimos en el perfilado: 25 inconsistencias `is_enterprise`/`plan_tier`, 89 usuarios con rol `admin` no documentado, 232 con `last_login < created_at`, 47 recursos con tags en conflicto, ruido de FX en billing. Van a la entrega final con los marts de Soporte/Revenue/NPS.

## 16. NPS = 101
Hay un `nps_score=101` (fuera de rango) en orgs. No nos afecta: usamos orgs sólo para enriquecer con industry/plan/region/lifecycle, y `nps_score` no entra en el mart. Se trataría al hacer el mart de NPS.

## 17. Dedupe doble
La dedupe del stream sólo cubre la ventana del watermark (10 min). Como los eventos abarcan ~60 días, agregamos un `dropDuplicates(["event_id"])` por lotes en Silver para unicidad global. El dataset igual no trae duplicados (43.200 event_id únicos), así que esto garantiza la propiedad más que eliminar filas.

## 18. Listar servicios en Q2
Q2 necesita saber qué servicios tiene una org. No lo hardcodeamos (un servicio nuevo quedaría afuera sin avisar) ni queremos escanear toda la tabla (gatilla un warning de Cassandra y no escala). Materializamos una tabla índice `services_by_org` con PK `((org_id), service)`: Q2 lee los servicios de una org en **una sola partición**, sin scan y siempre actualizada. Se puebla en la misma carga.

## 19. Tipo colección en Cassandra → `anomaly_methods set<text>`
La tabla usa una columna de tipo colección: `anomaly_methods set<text>`. Guarda qué métodos marcaron anomalía ese día (p. ej. `{'zscore','mad'}`). Un `set` encaja porque es un conjunto sin orden ni repetidos, y el dato ya lo calculamos en Silver. Lo agregamos al `CREATE TABLE`, al `INSERT` y a la agregación de Gold (`collect_set` + `flatten`).

## 20. Trade-off de la regla 3
La regla 3 manda a quarantine filas con `unit` nulo aunque tengan un `cost_usd_increment` válido, así que subestima el costo total en Gold. El notebook mide cuánto cuesta esa decisión (suma del cost de las filas quarantineadas con cost válido). La dejamos así porque la consigna pide esa regla, pero queda documentado. Una alternativa de producción: conservar el costo para la agregación y aislar sólo el `value` problemático.

## 21. Speed Layer a tabla propia
La capa de velocidad (`foreachBatch`) escribe a `org_daily_usage_stream`, **no** al mart del batch. Sólo calcula costo y requests; si escribiera a la tabla del batch, pondría en cero `cpu_hours`/`storage`/`genai` y pisaría lo que cargó el batch. Tabla separada = las dos vías conviven sin destruirse.

## 22. Performance
- Carga a Cassandra con `execute_concurrent_with_args` (concurrency=50), no un `execute` por fila — para 11k filas la diferencia es grande. La Speed Layer usa lo mismo.
- `broadcast` en los joins (orgs ~80 filas, stats ~6) → sin shuffle. Lo confirmamos con `explain("formatted")`: en el plan aparece `BroadcastHashJoin`.
- Compactación: el Bronze de streaming con `availableNow` escribe un Parquet por micro-batch, así que quedan archivos chicos. Para el MVP **decidimos no compactar** porque en Colab el volumen no lo justifica; en producción un `OPTIMIZE`/compactación post-ingesta los junta.

## 23. Verificar el plan con `explain`
Agregamos un `silver.explain("formatted")` después del join de enriquecimiento para confirmar que Spark usa `BroadcastHashJoin` (y no un shuffle join). Es la evidencia directa de que el `broadcast` que pedimos efectivamente se aplica.

## 24. Merge batch + speed en serving → fuera del MVP
En una Lambda completa la capa de serving mergea el resultado del batch (histórico completo) con el de la speed (lo más reciente) al consultar. No lo implementamos: servimos las dos vías en tablas separadas (`org_daily_usage_by_service` y `org_daily_usage_stream`) y el merge sería trabajo de la capa de serving en la entrega final. Para el MVP, con fuente estática, las dos calculan lo mismo, así que el merge no aportaría nada distinto todavía.
