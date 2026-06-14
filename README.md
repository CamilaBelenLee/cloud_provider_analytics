# Cloud Provider Analytics — Big Data 72.80 (ITBA, 1C 2026)

Pipeline analítico con **arquitectura Lambda** para un proveedor de nube: ingestar, limpiar, conformar y publicar datos para **FinOps**, **Soporte** y **Producto/GenAI**, usando **PySpark** + **Structured Streaming** en Colab, **Parquet** como almacenamiento intermedio y **Cassandra (AstraDB)** como capa de serving *query-first*.

**Integrantes:** Camila Lee (63382), Lucas Perri (62746)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/CamilaBelenLee/cloud_provider_analytics/blob/main/notebooks/cloud_provider_analytics_mvp.ipynb)

## Quickstart

1. Abrir `notebooks/cloud_provider_analytics_mvp.ipynb` en Google Colab.
2. **Datos:** correr la celda opcional *0b* (clona el repo y copia `datalake/landing/` a `/content/`), **o** subir el dataset a mano para tener `/content/datalake/landing/` con los 7 CSV y `usage_events_stream/*.jsonl`.
3. En AstraDB: crear una base **Serverless (Non-Vector)** y el keyspace **`cloud_analytics`**; generar un **application token** (`AstraCS:...`) y descargar el **Secure Connect Bundle** (zip). Subir el zip a `/content/`.
4. Cargá las credenciales: copiar `.env.example` a `.env` y completarlo, **o** cargar `SCB_PATH`, `ASTRA_TOKEN`, `ASTRA_KEYSPACE` en **Colab Secrets**.
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
│   ├── architecture.svg               # diagrama actualizado
│   ├── decision_log.md                # log de decisiones
│   └── evidence/                      # capturas (CQL+resultado, conteos, tamaños)
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
- **Bronze** — Parquet tipado, deduplicado, `ingest_ts` + `source_file`, particionado.
- **Silver** — limpieza, conformance, enriquecimiento (joins broadcast), 3 reglas de calidad + quarantine, flag de anomalía por servicio.
- **Gold** — marts de negocio, agregados, redondeados en el borde de serving.

## El dato (notas del esquema real)

- Los **eventos** traen `timestamp` (no `event_time`) y un campo `metric` (`requests` | `cpu_hours` | `storage_gb_hours`) que define qué mide `value`. `value` llega como número, como `"100.0"` o nulo → se lee **string** y se castea con fallback. `carbon_kg`/`genai_tokens` son **sólo v2** (después del 2025-07-18) → esquema unión nullable.
- **Facturación** trae `exchange_rate_to_usd` (fuente de FX), `credits` muchas veces vacío (→ 0), `subtotal` ocasionalmente negativo.
- Los **maestros** se unen por `org_id`; la región de la org es `hq_region` (los eventos tienen su propio `region`).

## Calidad de datos

Tres reglas sobre eventos: `event_id` no nulo; `cost_usd_increment ≥ -0.01`; `unit` no nulo cuando `value` existe. Las filas que fallan van a **quarantine** (se inspeccionan, no se descartan). La dedupe en streaming (ventana del watermark) se respalda con un `dropDuplicates(["event_id"])` por lotes para unicidad global.

## Detección de anomalías

**z-score**, **MAD** y **p99** por servicio; se marca anomalía sólo cuando **coinciden ≥2 de 3** (consenso → menos falsos positivos). Las anomalías se **marcan, no se eliminan ni recortan** — son los picos de costo reales (la señal de FinOps), que se exponen en lugar de ocultarse.

## Serving — por qué tablas CQL y no colecciones del Document API

La consigna pide modelado *query-first* en Cassandra con clave de partición/clustering. Modelamos **tablas CQL reales** (`org_daily_usage_by_service` con `PRIMARY KEY ((org_id, service), usage_date)`) vía el driver de DataStax + Secure Connect Bundle, cargadas con **UPSERTs** preparados (idempotentes). Deliberadamente **no** usamos el Document API (colecciones schemaless), que descartaría el modelado por clave de partición que el proyecto evalúa. Ver `docs/decision_log.md`.

## Alcance del MVP (Segundo Parcial)

Batch→Bronze (3 maestros) · Streaming→Bronze (eventos) · Silver (reglas + quarantine + features + anomalías) · mart FinOps en Gold · 1 tabla CQL · Q1 + Q2 · prueba de idempotencia · Speed Layer con `foreachBatch`. Los marts restantes (revenue, tickets, GenAI, cost-anomaly) y las consultas Q3–Q5 son el siguiente paso hacia la entrega final.

## Asunciones y riesgos

El dataset entra en Colab (~60 días de eventos; si crece, el mismo código Spark escala a un cluster sin cambios). El JSONL llega en micro-lotes de minutos → SLA near real-time, no sub-segundo estricto. `/content` es efímero → montar Drive para persistir Parquet/checkpoints. Late data acotada por watermark de 10 minutos. FX tomado de `exchange_rate_to_usd`.

## Limitaciones

Streaming basado en archivos (no Kafka); un único keyspace; el top-N de Q2 es un valor calculado, así que se sirve escaneando las particiones de la org y agregando del lado de la app (la alternativa de producción es un mart dedicado `top_services_by_org`).