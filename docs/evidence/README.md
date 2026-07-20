# Evidencia de ejecución

Salida textual del pipeline, un archivo por criterio. Existe para poder verificar los resultados **sin re-ejecutar el notebook ni tener credenciales de AstraDB**.

**Los escribe el propio pipeline.** La celda *Reporte de evidencia* del notebook corre cada criterio, lo imprime y lo escribe acá en la misma pasada. No se copian a mano — así los números de estos archivos son siempre los de la corrida que los produjo, que es lo que evita que un archivo diga 43.200 y otro 11.050.

| Archivo | Qué demuestra | Necesita AstraDB |
|---|---|---|
| [`c1.txt`](c1.txt) | Ingesta batch + streaming a Bronze, sin pérdida de eventos | no |
| [`c2.txt`](c2.txt) | Las 3 reglas de calidad y la quarantine, con el desglose por regla | no |
| [`c3.txt`](c3.txt) | Los 5 marts de Gold y las filas cargadas en Cassandra | parcial |
| [`c4.txt`](c4.txt) | **Q1** · costos y requests diarios por org y servicio, en un rango | sí |
| [`c5.txt`](c5.txt) | **Q2** · top-N servicios por costo acumulado, últimos 14 días | sí |
| [`c8_q3.txt`](c8_q3.txt) | **Q3** · tickets críticos y tasa de SLA breach por día | sí |
| [`c9_q4.txt`](c9_q4.txt) | **Q4** · revenue mensual con créditos e impuestos, en USD | sí |
| [`c10_q5.txt`](c10_q5.txt) | **Q5** · tokens GenAI y costo estimado por día | sí |
| [`c6.txt`](c6.txt) | Idempotencia: conteos antes y después de re-cargar, en las 6 tablas | sí |
| [`c7.txt`](c7.txt) | Rutas y tamaños en disco de cada zona, con sus particiones | no |
| [`fx.txt`](fx.txt) | La medición detrás de la decisión de FX (§D25) | no |
| [`watermark.txt`](watermark.txt) | La medición detrás del watermark de 60 días (§D8) | no |
| [`explain_broadcast.txt`](explain_broadcast.txt) | Plan físico con `BroadcastHashJoin`: el broadcast se aplica de verdad | no |

## Los números y su significado

| | |
|---|---|
| Eventos en el landing | 43.200 (43.200 `event_id` únicos) |
| Eventos en Bronze | 43.200 — sin pérdida |
| Silver válidas / quarantine | 40.956 / 2.244 (suman 43.200) |
| Anomalías marcadas (≥2 de 3 métodos) | 89 |
| Gold `org_daily_usage_by_service` | 11.050 |
| Gold `tickets_by_org_date` | 944 |
| Gold `revenue_by_org_month` | 240 |
| Gold `genai_tokens_by_org_date` | 1.131 |
| Gold `cost_anomaly_mart` | 89 |

## Cómo se regeneran

Los que no necesitan AstraDB salen de correr el notebook hasta Gold. Los demás requieren credenciales: correr el notebook completo en Colab y bajar la carpeta con la celda **10b**, que la empaqueta y la descarga.

Ver el [quickstart del README](../../README.md#quickstart-pasos-exactos).
