-- ================================================================================
-- VISTAS ANALÍTICAS — SALUD MENTAL TRABAJO REMOTO
-- ================================================================================
-- Vistas preconstruidas para consultas frecuentes de KPIs.
-- Ejecutar después del pipeline ETL.
--
-- Autor: Gerardo Suárez T. (gtsuarez-analytics)
-- Proyecto: Salud Mental en el Trabajo Remoto
-- ================================================================================

-- ── Vista: KPIs Globales ─────────────────────────────────────────
CREATE OR REPLACE VIEW v_kpis_globales AS
SELECT
    ROUND(AVG(indice_bienestar), 2)                                                          AS indice_bienestar_promedio,
    ROUND(SUM(CASE WHEN nivel_estres >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)        AS tasa_alto_estres_pct,
    ROUND(SUM(CASE WHEN accede_soporte = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)    AS cobertura_soporte_pct,
    ROUND(SUM(CASE WHEN productividad >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)      AS productividad_sostenida_pct,
    COUNT(*)                                                                                  AS total_registros
FROM fact_bienestar_laboral;

-- ── Vista: KPIs por Modalidad ────────────────────────────────────
CREATE OR REPLACE VIEW v_kpis_por_modalidad AS
SELECT
    t.modalidad,
    COUNT(*)                                                                                  AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)                                                        AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)                                                            AS estres_promedio,
    ROUND(AVG(f.balance_work_life), 2)                                                        AS balance_promedio,
    ROUND(AVG(f.productividad), 2)                                                            AS productividad_promedio,
    ROUND(SUM(CASE WHEN f.nivel_estres >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)      AS tasa_alto_estres_pct,
    ROUND(SUM(CASE WHEN f.productividad >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)    AS productividad_sostenida_pct
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
GROUP BY t.modalidad;

-- ── Vista: Brecha de Género ──────────────────────────────────────
CREATE OR REPLACE VIEW v_brecha_genero AS
SELECT
    t.modalidad,
    d.genero,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)  AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)      AS estres_promedio
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
INNER JOIN dim_demografia d ON f.id_demografia = d.id_demografia
WHERE d.genero IN ('Male', 'Female')
GROUP BY t.modalidad, d.genero;
