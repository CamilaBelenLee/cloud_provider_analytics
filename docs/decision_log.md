# Log de decisiones — Cloud Provider Analytics

Cada entrada: la decisión, las alternativas y la justificación.

## 1. Lambda vs Kappa → **Lambda**
Los CSV de maestros/facturación/encuestas son **estados** (cadencia humana/de período); `usage_events_stream` son **eventos** (fragmentados para simular un feed en vivo). Lambda procesa cada flujo según su naturaleza. Kappa exigiría capturar cambios sobre CSV estáticos más un broker durable y replayable (Kafka), fuera del stack provisto.

## 2. Serving: tablas **CQL** reales, no colecciones del Document API ?? esto no se si hace falta defenderlo por que es lo que pide la consigna literalmente
La consigna pide modelado *query-first* en Cassandra con clave de partición/clustering. Las tablas CQL nos dan `PRIMARY KEY ((org_id, service), usage_date)` y una historia de modelado defendible. Las colecciones (astrapy) cargan más fácil pero son schemaless y **sin modelado por clave de partición** — descartarían un concepto evaluado. Aceptamos algo más de fricción de carga a cambio de cumplir la consigna.

## 3. Particionado → Parquet particionado por `usage_date` (y `service` en Silver)
Las consultas filtran por fecha y servicio, así que particionar por esas columnas habilita *partition pruning* (leer sólo las carpetas relevantes). En Colab, `spark.sql.shuffle.partitions=8` (dato chico; el default 200 generaría tareas diminutas).

## 4. Claves Cassandra → `PRIMARY KEY ((org_id, service), usage_date)`
Q1 ("costo diario por org+servicio en un rango") es una lectura de una sola partición ordenada por fecha: clave de partición `(org_id, service)` (siempre en el `WHERE`), clave de clustering `usage_date DESC` para rangos. Una tabla por consulta (denormalizado, *query-first*).

## 5. `value` leído como **string** y casteado con fallback
`value` en el archivo ... tiene tipo inconsistente (número, `"100.0"`, nulo). Declararlo `double` haría que Spark anule las filas string al parsear. Lo leemos string (no perdemos nada), conservamos el valor crudo para evidencia en quarantine, y `cast("double")` a `value_num` bajo nuestro control.

## 7. Umbrales de anomalía → por servicio, **≥2 de 3** métodos
z-score (`>3σ`), MAD (`>3` en la escala `1.4826·MAD`), p99 (`> p99·1.5`). Estadísticos **por servicio** (las escalas de costo difieren). Se marca sólo si coinciden ≥2: cada método tiene puntos ciegos (el z-score se distorsiona con los propios outliers; p99 es tosco), el consenso reduce falsos positivos. `K=1.5` es la única perilla elegida libremente — ajustable. **Marcamos, no recortamos**: las anomalías son la señal de FinOps, no datos a eliminar.

## 8. Watermark → 10 minutos
Acota el estado del streaming y la tolerancia a late data. *(Clase Spark Structured Streaming "watermark = max event time − retraso permitido".)* Coincide con la asunción de que el JSONL llega en micro-lotes de minutos.

## 9. Trigger → `availableNow`
La fuente es una carpeta fija de archivos JSONL. *(`availableNow=True` "procesa lo disponible y termina".)* Corre toda la maquinaria de streaming (watermark, dedupe con estado, checkpoint) y se detiene — reproducible y permite seguir el notebook. En producción contra un feed vivo se cambiaría a `processingTime`.

## 10. Idempotencia → checkpoint + claves naturales + UPSERT
*("Fuente replayable + checkpoint + sink idempotente/transaccional = base para semántica exactly-once end-to-end".)* Archivos replayables; `event_id` y `(org_id, service, usage_date)` como claves naturales; INSERT==UPSERT en Cassandra sobreescribe por clave. Re-ejecutar no cambia el conteo (se prueba en el notebook).

## 11. Output mode → `append` (ingesta) / `update` (agregación)
*("si el resultado todavía puede cambiar por eventos tardíos, append sólo es seguro… [con] una ventana cerrada por watermark".)* La ingesta a Bronze no agrega → `append`. La agregación diaria de la Speed Layer cambia con cada micro-batch → `update`, combinado con el UPSERT de Cassandra.

## 12. Normalización de FX → multiplicar por `exchange_rate_to_usd`??
Revenue normalizado vía el `exchange_rate_to_usd` por factura; `credits` lleva `coalesce(...,0)`. Las filas USD traen tasas ~1.0 con ruido; usamos el campo tal cual y dejamos documentada la elección (alternativa: forzar USD→1.0). (El mart de revenue es parte de la entrega final, no del MVP.)

## 13. Dedupe → en streaming (watermark) + por lotes (global)
El `dropDuplicates(["event_id"])` en streaming sólo deduplica dentro de la ventana del watermark. Un `dropDuplicates` por lotes en Silver garantiza unicidad global para los marts. Es red de seguridad deliberada, no redundancia.