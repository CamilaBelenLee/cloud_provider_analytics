# Cloud Provider Analytics — Big Data 72.80 (ITBA, 1C 2026)

Pipeline analítico con **arquitectura Lambda** para un proveedor de nube: ingestar, limpiar, conformar y publicar datos para **FinOps**, **Soporte** y **Producto/GenAI**, usando **PySpark** + **Structured Streaming** en Colab, **Parquet** como almacenamiento intermedio y **Cassandra (AstraDB)** como capa de serving *query-first*.

**Integrantes:** Camila Lee (63382), Lucas Perri (62746)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/CamilaBelenLee/cloud_provider_analytics/blob/main/notebooks/cloud_provider_analytics_mvp.ipynb)

## Quickstart (pasos exactos)

1. Abrir el notebook en Colab con la insignia de arriba (o *Archivo → Abrir notebook → GitHub* y pegar la URL del repo).
2. **Datos:** correr la celda opcional *0b* (clona el repo y copia `datalake/landing/` a `/content/`), **o** subir el dataset a mano para tener `/content/datalake/landing/` con los 7 CSV y `usage_events_stream/*.jsonl`.
3. En AstraDB: crear una base **Serverless (Non-Vector)** y el keyspace **`cloud_analytics`**; generar un **application token** (`AstraCS:...`) y descargar el **Secure Connect Bundle** (zip). Subir el zip a `/content/`.
4. Cargar las credenciales **sin hardcodear**: copiar `.env.example` a `.env` y completarlo, **o** cargar `SCB_PATH`, `ASTRA_TOKEN`, `ASTRA_KEYSPACE` en **Colab Secrets**.
5. *Entorno de ejecución → Ejecutar todo.* El notebook corre `Landing → Bronze → Silver → Gold → Cassandra`, ejecuta Q1 y Q2, y muestra la prueba de idempotencia.

```
cloud-provider-analytics/
├── README.md
├── requirements.txt
├── .env.example                       # plantilla de credenciales (copiar a .env)
├── .gitignore                         # excluye zonas generadas + .env + el SCB
├── notebooks/
│   └── cloud_provider_analytics_mvp.ipynb
├── cql/
│   └── schema.cql
├── docs/
│   ├── arquitecture.png               # diagrama de arquitectura
│   ├── data_dictionary.md             # diccionario de datos (campos, tipos, dominio)
│   ├── decision_log.md                # log de decisiones
│   └── evidence/                      # salida textual de cada criterio de aceptación
│       ├── c1.txt                     # conteos batch + streaming a Bronze
│       ├── c2.txt                     # reglas de calidad y quarantine (ejemplos de filas)
│       ├── c3.txt                     # mart Gold + tabla Cassandra poblada (top anomalías)
│       ├── c4.txt                     # Q1 — CQL ejecutado + resultado desde AstraDB
│       ├── c5.txt                     # Q2 — top-N servicios por costo (ventana 14d)
│       ├── c6.txt                     # idempotencia (conteo antes/después = igual)
│       └── c7.txt                     # rutas en disco y tamaños del particionado
└── datalake/
    └── landing/                       # datos provistos (se versionan)
    #   bronze/ silver/ gold/ quarantine/ se GENERAN al correr (no se versionan)
```

## Arquitectura

**Lambda**, organizada sobre zonas medallion en un Data Lake.

- **Capa batch** — los 7 CSV de maestros/facturación/encuestas (estados, cadencia humana/de período).
- **Capa de velocidad (speed)** — `usage_events_stream/*.jsonl` vía Structured Streaming (eventos; watermark, dedupe por `event_id`, checkpoint). Incluye la variante con `foreachBatch` que agrega por ventana diaria y escribe a Cassandra.
- **Capa de serving** — tablas **CQL** en AstraDB; una tabla por consulta de negocio.

### Zonas del Data Lake
- **Landing** — archivos originales, inmutables.
- **Bronze** — Parquet tipado y deduplicado, con `ingest_ts` + `source_file` de procedencia. Mismo grano que la fuente; particionado.
- **Silver** — limpieza, 3 reglas de calidad con quarantine, enriquecimiento por broadcast join con las dimensiones, y el flag de anomalía por servicio (z-score/MAD/p99, marca is >= 2 coinciden).
- **Gold** — marts listos para servir.

## Datos (notas del esquema real)

- Los **eventos** traen `timestamp` (no `event_time`) y un campo `metric` (`requests` | `cpu_hours` | `storage_gb_hours`) que define qué mide `value`. `value` llega como número, como `"100.0"` o nulo → se lee **string** y se castea con fallback. `carbon_kg`/`genai_tokens` son **sólo v2** (después del 2025-07-18) → esquema unión nullable.
- **Facturación** trae `exchange_rate_to_usd` (fuente de FX), `credits` muchas veces vacío (→ 0), `subtotal` ocasionalmente negativo.
- Los **maestros** se unen por `org_id`; la región de la org es `hq_region` (los eventos tienen su propio `region`).

## Calidad de datos

Tres reglas sobre eventos: `event_id` no nulo; `cost_usd_increment ≥ -0.01`; `unit` no nulo cuando `value` existe. Las filas que fallan van a **quarantine** (se inspeccionan, no se descartan). La dedupe en streaming (ventana del watermark) se respalda con un `dropDuplicates(["event_id"])` por lotes para unicidad global.

## Detección de anomalías

**z-score**, **MAD** y **p99** por servicio; se marca anomalía sólo cuando **coinciden ≥2 de 3** (consenso → menos falsos positivos). Las anomalías se **marcan, no se borran** — son los picos de costo que FinOps quiere ver.

## Serving — tablas CQL (con una columna colección)

Usamos tablas CQL, no el Document API de Astra, porque así podemos definir la partition key — que es lo que la consigna evalúa (modelado query-first). La tabla `org_daily_usage_by_service` tiene `PRIMARY KEY ((org_id, service), usage_date)` y se carga con UPSERTs preparados desde Spark (idempotentes).

La tabla incluye una columna de **tipo colección** de CQL: `anomaly_methods set<text>`, que guarda qué métodos (zscore/mad/p99) marcaron anomalía ese día. Ver `docs/decision_log.md`.

## Evidencia (`docs/evidence/`)

La carpeta `docs/evidence/` guarda la salida textual de la celda "Reporte de evidencia" del notebook, partida en un archivo por criterio de aceptación. Existe para que el evaluador pueda verificar los resultados **sin re-ejecutar** el notebook ni tener acceso a AstraDB.

| Archivo | Qué demuestra |
|---|---|
| `c1.txt` | Que los 3 maestros y los eventos se ingestan a Bronze con los conteos correctos (80 orgs, 800 users, 240 billing, 43200 eventos). |
| `c2.txt` | Que las 3 reglas de calidad funcionan: 40956 filas válidas, 2244 en quarantine, con ejemplos concretos de filas rechazadas (unit=NULL con value presente). |
| `c3.txt` | Que el mart Gold tiene 11050 filas y coincide con lo cargado en Cassandra. Muestra las filas de mayor costo (todas marcadas como anomalía por los 3 métodos). |
| `c4.txt` | Q1 ejecutada contra AstraDB: CQL con la query exacta y el resultado (costo+requests diarios por org+servicio en un rango). |
| `c5.txt` | Q2 ejecutada contra AstraDB: top servicios por costo acumulado en los últimos 14 días para una org. |
| `c6.txt` | Idempotencia: conteo antes y después de re-cargar = 11050 en ambos casos. El UPSERT no duplica. |
| `c7.txt` | Rutas físicas de cada zona con tamaños (`du -sh`) y carpetas de partición (`usage_date=...`), demostrando que el Parquet está particionado. |

## Alcance del MVP (Segundo Parcial)

Batch→Bronze (3 maestros) · Streaming→Bronze (eventos) · Silver (reglas + quarantine + features + anomalías) · mart FinOps en Gold · 1 tabla CQL · Q1 + Q2 · prueba de idempotencia · Speed Layer con `foreachBatch`. Los marts restantes (revenue, tickets, GenAI, cost-anomaly) y las consultas Q3–Q5 son el siguiente paso hacia la entrega final.

## Asunciones y riesgos

El dataset entra en Colab (~60 días de eventos; si crece, el mismo código Spark escala a un cluster sin cambios). El JSONL llega en micro-lotes de minutos → SLA near real-time, no sub-segundo estricto. `/content` es efímero → montar Drive para persistir Parquet/checkpoints. Late data acotada por watermark de 10 minutos. FX tomado de `exchange_rate_to_usd`.

## Limitaciones

Streaming basado en archivos (no Kafka); un único keyspace; el top-N de Q2 es un valor calculado, así que se sirve escaneando las particiones de la org y agregando del lado de la app (la alternativa de producción es un mart dedicado `top_services_by_org`).