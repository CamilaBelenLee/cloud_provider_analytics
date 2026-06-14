# Log de decisiones — Cloud Provider Analytics

Cada entrada: la decisión, las alternativas y el porqué.

## 1. Lambda vs Kappa → **Lambda**
Los CSV de maestros/facturación/encuestas son **estados** (cadencia humana/de período); `usage_events_stream` son **eventos** (fragmentados para simular un feed en vivo). Lambda procesa cada flujo según su naturaleza. Kappa exigiría capturar cambios sobre CSV estáticos más un broker durable y replayable (Kafka), fuera del stack provisto.

## 2. Serving: tablas **CQL** reales, no colecciones del Document API
La consigna pide modelado *query-first* en Cassandra con clave de partición/clustering. Las tablas CQL nos dan `PRIMARY KEY ((org_id, service), usage_date)` y una historia de modelado defendible. Las colecciones (astrapy) cargan más fácil pero son schemaless y **sin modelado por clave de partición** — descartarían un concepto evaluado.

## 3. Particionado → Parquet particionado en todas las zonas
**Maestros (Bronze):** orgs por `hq_region`, users por `role`, billing por `currency` (columnas reales de baja cardinalidad → carpetas `hq_region=us-east/`, etc.). **Eventos/Silver:** por `usage_date` y `service`. **Gold:** por `usage_date`. Esto habilita *partition pruning* (leer sólo las carpetas relevantes) y da la evidencia de "rutas y tamaños". En Colab, `spark.sql.shuffle.partitions=8` (dato chico; el default 200 generaría tareas diminutas).

## 4. Claves Cassandra → `PRIMARY KEY ((org_id, service), usage_date)`
Q1 ("costo diario por org+servicio en un rango") es una lectura de una sola partición ordenada por fecha: clave de partición `(org_id, service)` (siempre en el `WHERE`), clave de clustering `usage_date DESC` para rangos. Una tabla por consulta (denormalizado, *query-first*).

## 5. Features derivadas de `metric`, no conteo de filas
`requests`/`cpu_hours`/`storage_gb_hours` se calculan como `sum(value WHERE metric=X)`. Un `count(*)` de filas etiquetaría mal "cantidad de eventos" como "requests" (bug real que evitamos).

## 6. `value` leído como **string** y casteado con fallback
`value` tiene tipo inconsistente (número, `"100.0"`, nulo; en el perfilado: ~1.300 string, ~880 nulos). Declararlo `double` haría que Spark anule las filas string al parsear. Lo leemos string, conservamos el valor crudo para evidencia en quarantine, y `cast("double")` a `value_num` bajo nuestro control.

## 7. Umbrales de anomalía → por servicio, **≥2 de 3** métodos
z-score (`>3σ`), MAD (`>3` en la escala `1.4826·MAD`), p99 (`> p99·1.5`). Estadísticos **por servicio** (las escalas de costo difieren). Se marca sólo si coinciden ≥2: cada método tiene puntos ciegos (el z-score se distorsiona con los propios outliers; p99 es tosco), el consenso reduce falsos positivos. `K=1.5` es la única perilla elegida libremente. **Marcamos, no recortamos**: las anomalías son la señal de FinOps. (El dataset trae 211 costos negativos y picos hasta ~317; el consenso deja ~89 marcados.)

## 8. Watermark → 10 minutos
Acota el estado del streaming y la tolerancia a late data. *(watermark = max event time − retraso permitido)*

## 9. Trigger → `availableNow`
Fuente fija de archivos JSONL. *(`availableNow=True` "procesa lo disponible y termina")* Corre toda la maquinaria de streaming y se detiene — reproducible. En producción contra un feed en vivo se cambiaría a `processingTime`.

## 10. Idempotencia → checkpoint + claves naturales + UPSERT
*("Fuente replayable + checkpoint + sink idempotente = base para exactly-once end-to-end".)* Archivos replayables; `event_id` y `(org_id, service, usage_date)` como claves naturales; INSERT==UPSERT en Cassandra. Re-ejecutar no cambia el conteo (se prueba en el notebook).

## 11. Output mode → `append` (ingesta) / `update` (agregación)
*("append sólo es seguro… [con] una ventana cerrada por watermark".)* La ingesta a Bronze no agrega → `append`. La agregación diaria de la Speed Layer cambia con cada micro-batch → `update`, combinado con el UPSERT de Cassandra.

## 12. Conformance de categóricos → inspeccionado, sin transformación cosmética
Inspeccionamos `service` y `region`: los valores ya vienen consistentes (`compute`, `us-east`, …), sin variación de mayúsculas/espacios. Decidimos **no** agregar `upper/trim` para no introducir transformaciones innecesarias. Si apareciera ruido de casing, la normalización iría en Silver. (Decisión consciente, no omisión.)

## 13. Tratamiento de nulos → política explícita
Los nulos que **violan una regla de negocio** (ej. `unit` ausente con `value` presente, ~2.038 filas) van a **quarantine**. Los nulos **legítimos** se conservan como NULL (no se inventan datos): `value` nulo (~880), `nps_score`/`csat` nulos. Las features numéricas se llevan a 0 **sólo en Gold**, donde 0 = "sin actividad ese día".

## 14. SCD → **no aplica**
Evaluamos SCD Type 2 para las dimensiones. **No aplica**: los maestros son un *snapshot único* (no hay múltiples versiones de una misma org/usuario/recurso en el tiempo), así que no hay historial que capturar. Forzarlo agregaría complejidad sin valor. Queda como mejora futura si los maestros pasaran a entregarse como series temporales.

## 15. Alcance de calidad → qué ataca el MVP y qué se difiere
El MVP ataca las issues que tocan el **mart FinOps** (eventos + `org_id`): `value` string/nulo, `unit` nulo con value (regla 3 → quarantine), costos negativos/picos (anomalías), dedupe por `event_id`, esquema v1/v2 (corte 2025-07-18). Se **documentan pero no se tratan** las issues de archivos fuera del MVP: `is_enterprise` vs `plan_tier` inconsistente (25), rol `admin` no documentado (89, aceptado por `role` como `StringType`), `last_login < created_at` (232), tags en conflicto en resources (47), `credits` vacíos / subtotales negativos / ruido de FX en USD (billing). Se atacarán al construir los marts de Soporte/Revenue/NPS en la entrega final.

## 16. NPS = 101 (fuera de rango) → no contamina el MVP
`customers_orgs` tiene un `nps_score=101` (fuera del rango -100..100). **No se propaga al mart FinOps** porque sólo usamos `customers_orgs` para enriquecer con `industry`/`plan_tier`/`hq_region`/`lifecycle_stage` — `nps_score` no entra en el enriquecimiento ni en el mart. Se tratará (clamp/quarantine) al construir el mart de NPS en la final.

## 17. Dedupe → en streaming (watermark) + por lotes (global)
El `dropDuplicates(["event_id"])` en streaming sólo deduplica dentro de la ventana del watermark (10 min). Como los eventos abarcan ~60 días, agregamos un `dropDuplicates` por lotes en Silver para unicidad global. (El dataset no trae duplicados — los 43.200 `event_id` son únicos — así que esto garantiza la propiedad por diseño, no elimina filas.)

## 18. Listado de servicios para Q2 → scan acotado en el MVP, índice en producción
Q2 ("top-N servicios por costo, últimos 14 días") necesita saber **qué servicios tiene una org**. Como la clave de partición es el par `(org_id, service)`, Cassandra **no** permite preguntar eficientemente "todos los servicios donde `org_id=X`" (eso requeriría escanear todas las particiones). Evaluamos tres opciones:
- **Hardcodear el catálogo de servicios** → descartado: si el proveedor agrega un servicio nuevo, quedaría fuera del top-N **silenciosamente** (un resultado incorrecto que parece correcto).
- **Tabla de índice `services_by_org`** con `PRIMARY KEY ((org_id), service)` → la solución *query-first* correcta y escalable: `SELECT service WHERE org_id=?` es una lectura de una sola partición, se puebla por UPSERT al cargar el Gold (los servicios nuevos entran solos). Documentada en `schema.cql` como evolución.
- **Scan acotado + filtro en la app** (elegida para el MVP) → siempre actualizada (descubre cualquier servicio nuevo porque lee todo) y aceptable por el volumen (~11k filas). Su único defecto es que no escala; en producción se reemplaza por la tabla de índice. Preferimos el scan sobre el hardcode porque el scan **nunca se desactualiza**.