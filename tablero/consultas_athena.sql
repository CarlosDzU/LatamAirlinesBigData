-- Consultas de validación y de los indicadores del tablero (workgroup latam-ops, base de datos latam_ops).
-- Las tablas las crea el crawler de Glue sobre gold/ (make catalogar).

-- K1 puntualidad de salida por hub y mes
SELECT year, month, origen_icao,
       SUM(salidas_realizadas)                                  AS realizadas,
       ROUND(SUM(k1_puntualidad * salidas_realizadas) / NULLIF(SUM(salidas_realizadas), 0), 4) AS k1_puntualidad
FROM kpi_diario
WHERE origen_icao IN ('SBGR', 'SBSP', 'SBBR')
GROUP BY year, month, origen_icao
ORDER BY year, month, origen_icao;

-- K2 alertas emitidas por día en Guarulhos (último mes cargado)
SELECT fecha, k2_alertas_ambar, k2_alertas_rojas, salidas_programadas
FROM kpi_diario
WHERE origen_icao = 'SBGR'
  AND year = (SELECT MAX(year) FROM kpi_diario)
ORDER BY fecha;

-- K3 índice de propagación: fracción de salidas ya atrasadas en el aeródromo en [T-4 h, T-2 h)
SELECT origen_icao, ROUND(AVG(k3_indice_propagacion), 4) AS k3_promedio
FROM kpi_diario
GROUP BY origen_icao
HAVING SUM(salidas_programadas) > 1000
ORDER BY k3_promedio DESC
LIMIT 15;

-- K4 calidad de la alerta roja por mes
SELECT anio_mes, k4_precision, k4_recall, vp, fp, fn
FROM kpi_alerta_mensual
ORDER BY anio_mes;

-- Linaje: de un valor del tablero a sus vuelos de origen (auditable hasta el CSV de la ANAC vía id_vuelo)
SELECT f.id_vuelo, f.numero_voo, f.origen_key, f.destino_key, f.partida_prevista_utc, f.dep_delay_min, f.nivel_alerta
FROM fact_vuelo f
WHERE f.fecha_key = 20250115 AND f.origen_key = 'SBGR'
ORDER BY f.partida_prevista_utc
LIMIT 50;

-- Demora media por condición meteorológica a T-2 h (relación clima -> demora)
SELECT c.categoria_vuelo, COUNT(*) AS vuelos, ROUND(AVG(f.es_demora15), 4) AS tasa_demora15
FROM fact_vuelo f
JOIN dim_condicion_clima c ON f.condicion_clima_key = c.condicion_clima_key
WHERE f.cancelado = 0
GROUP BY c.categoria_vuelo
ORDER BY tasa_demora15 DESC;
