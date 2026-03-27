-- ================================================================================
-- CONSULTAS ANALÍTICAS — SALUD MENTAL TRABAJO REMOTO
-- ================================================================================
-- Todas las preguntas analíticas (PA-01 a PA-06) en un solo archivo.
-- Ejecutar después del pipeline ETL.
--
-- Autor: Gerardo Suárez T. (gtsuarez-analytics)
-- Proyecto: Salud Mental en el Trabajo Remoto
-- ================================================================================

-- ================================================================================
-- PA-01: ¿El trabajo remoto genera mayor deterioro de salud mental?
-- ================================================================================
SELECT
    t.modalidad,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)  AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)      AS estres_promedio,
    ROUND(AVG(f.balance_work_life), 2)  AS balance_promedio,
    ROUND(SUM(CASE WHEN f.nivel_estres >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS tasa_alto_estres_pct
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
GROUP BY t.modalidad
ORDER BY indice_bienestar ASC;

-- ================================================================================
-- PA-02: ¿Qué industrias remotas tienen peor impacto en salud mental?
-- ================================================================================
SELECT
    t.industria,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)  AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)      AS estres_promedio,
    ROUND(SUM(CASE WHEN f.nivel_estres >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS tasa_alto_estres_pct
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
WHERE t.modalidad = 'Remote'
GROUP BY t.industria
HAVING COUNT(*) >= 30
ORDER BY indice_bienestar ASC
LIMIT 10;

-- ================================================================================
-- PA-03: ¿Existe una brecha de género en el impacto del trabajo remoto?
-- ================================================================================
SELECT
    t.modalidad,
    d.genero,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)  AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)      AS estres_promedio,
    ROUND(AVG(f.productividad), 2)     AS productividad_promedio
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
INNER JOIN dim_demografia d ON f.id_demografia = d.id_demografia
WHERE d.genero IN ('Male', 'Female')
GROUP BY t.modalidad, d.genero
ORDER BY t.modalidad, estres_promedio DESC;

-- ================================================================================
-- PA-04: ¿El acceso a soporte de salud mental mejora la productividad?
-- ================================================================================
SELECT
    t.modalidad,
    f.accede_soporte,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)  AS indice_bienestar,
    ROUND(AVG(f.productividad), 2)      AS productividad_promedio,
    ROUND(SUM(CASE WHEN f.productividad >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS productividad_sostenida_pct
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
GROUP BY t.modalidad, f.accede_soporte
ORDER BY t.modalidad, f.accede_soporte;

-- ================================================================================
-- PA-05: ¿A partir de cuántas horas el deterioro se acelera?
-- ================================================================================
WITH tramos_horas AS (
    SELECT
        f.*,
        t.modalidad,
        t.horas_semanales,
        CASE
            WHEN t.horas_semanales IS NULL THEN '5. Sin datos'
            WHEN t.horas_semanales <= 35 THEN '1. <=35h (Part-time)'
            WHEN t.horas_semanales <= 40 THEN '2. 36-40h (Normal)'
            WHEN t.horas_semanales <= 45 THEN '3. 41-45h (Extendido)'
            WHEN t.horas_semanales <= 50 THEN '4. 46-50h (Sobre tiempo)'
            ELSE '5. >50h (Exceso)'
        END AS tramo_horas
    FROM fact_bienestar_laboral f
    INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
)
SELECT
    modalidad,
    tramo_horas,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(indice_bienestar), 2)     AS indice_bienestar,
    ROUND(AVG(nivel_estres), 2)         AS estres_promedio
FROM tramos_horas
GROUP BY modalidad, tramo_horas
ORDER BY modalidad,
    CASE WHEN tramo_horas LIKE '1.%' THEN 1
         WHEN tramo_horas LIKE '2.%' THEN 2
         WHEN tramo_horas LIKE '3.%' THEN 3
         WHEN tramo_horas LIKE '4.%' THEN 4
         ELSE 5 END;

-- ================================================================================
-- PA-06: ¿La edad es un factor de riesgo para el deterioro bajo trabajo remoto?
-- ================================================================================
SELECT
    t.modalidad,
    d.rango_etario,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)  AS indice_bienestar,
    ROUND(AVG(f.balance_work_life), 2)  AS balance_promedio,
    ROUND(AVG(f.nivel_estres), 2)      AS estres_promedio
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
INNER JOIN dim_demografia d ON f.id_demografia = d.id_demografia
GROUP BY t.modalidad, d.rango_etario
HAVING COUNT(*) >= 10
ORDER BY t.modalidad, d.rango_etario;
