# Consultas Analíticas y KPIs — Salud Mental en el Trabajo Remoto

*Documento técnico para análisis de datos. Cada pregunta responde a una hipótesis de negocio.*

---

## 📊 KPIs Principales

| # | KPI | Fórmula | Meta | Fuente |
|---|-----|---------|------|--------|
| 1 | **Índice de Bienestar Compuesto** | `(6 - nivel_estres + balance_vida_trabajo + productividad) / 3 * 2` | ≥ 7.0 | `fact_bienestar_laboral` |
| 2 | **Tasa de Alto Estrés** | `COUNT(nivel_estres >= 4) / COUNT(*) * 100` | < 20% | `fact_bienestar_laboral` |
| 3 | **Cobertura de Soporte Mental** | `COUNT(accede_soporte = true) / COUNT(*) * 100` | ≥ 80% | `fact_bienestar_laboral` |
| 4 | **Tasa de Productividad Sostenida** | `COUNT(productividad >= 4) / COUNT(*) * 100` | ≥ 60% | `fact_bienestar_laboral` |

### Tabla resumen de KPIs

```sql
-- Vista de KPIs globales
CREATE VIEW v_kpis_globales AS
SELECT
    ROUND(AVG(indice_bienestar), 2)                         AS indice_bienestar_promedio,
    ROUND(SUM(CASE WHEN nivel_estres >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS tasa_alto_estres_pct,
    ROUND(SUM(CASE WHEN accede_soporte = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS cobertura_soporte_pct,
    ROUND(SUM(CASE WHEN productividad >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS productividad_sostenida_pct
FROM fact_bienestar_laboral;
```

---

## ❓ Preguntas Analíticas

Cada pregunta sigue la estructura:
- **Hipótesis**: Lo que esperamos encontrar
- **Query SQL**: Consulta para validar
- **Acción**: Qué hacer si se confirma

---

### PA-01: ¿El trabajo remoto genera mayor deterioro de salud mental que el presencial?

**Hipótesis**: El trabajo 100% remoto está asociado con mayores niveles de estrés y peor balance vida-trabajo.

**Query SQL**:
```sql
-- Comparación de KPIs por modalidad de trabajo
SELECT
    t.modalidad,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)   AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)       AS estres_promedio,
    ROUND(AVG(f.balance_work_life), 2)   AS balance_promedio,
    ROUND(SUM(CASE WHEN f.nivel_estres >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS tasa_alto_estres_pct
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
GROUP BY t.modalidad
ORDER BY indice_bienestar ASC;
```

**Métricas esperadas**:
- Estrés promedio por modalidad (1-5)
- Índice de bienestar por modalidad (1-10)

**Insight que aporta**: Identifica qué modalidad de trabajo está más asociada con deterioro de salud mental.

---

### PA-02: ¿Existen industrias donde el trabajo remoto impacta más la salud mental?

**Hipótesis**: Certain industries (tech, healthcare) have worse mental health outcomes for remote workers.

**Query SQL**:
```sql
-- Industrias con mayor impacto negativo (solo remote workers)
SELECT
    t.industria,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)   AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)       AS estres_promedio,
    ROUND(SUM(CASE WHEN f.nivel_estres >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS tasa_alto_estres_pct
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
WHERE t.modalidad = 'Remote'
GROUP BY t.industria
HAVING COUNT(*) >= 30  -- mínimo de muestra para significancia estadística
ORDER BY indice_bienestar ASC
LIMIT 10;
```

**Acción si se confirma**: Recursos humanos focalizados en esas industrias con programas de soporte especializados.

---

### PA-03: ¿Existe una brecha de género en el impacto del trabajo remoto?

**Hipótesis**: Las mujeres reportan mayor estrés y peor bienestar que los hombres bajo trabajo remoto.

**Query SQL**:
```sql
-- Brecha de género por modalidad (solo Male/Female para clean comparison)
SELECT
    t.modalidad,
    d.genero,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)   AS indice_bienestar,
    ROUND(AVG(f.nivel_estres), 2)       AS estres_promedio,
    ROUND(AVG(f.productividad), 2)       AS productividad_promedio
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
INNER JOIN dim_demografia d ON f.id_demografia = d.id_demografia
WHERE d.genero IN ('Male', 'Female')
GROUP BY t.modalidad, d.genero
ORDER BY t.modalidad, estres_promedio DESC;
```

**Insight que aporta**: Revela si el trabajo remoto afecta desproporcionadamente a un género, inform políticas de equidad.

---

### PA-04: ¿El acceso a soporte de salud mental mejora la productividad?

**Hipótesis**: Empleados con acceso a soporte psicológico tienen mayor productividad sostenida.

**Query SQL**:
```sql
-- Productividad según acceso a soporte, por modalidad
SELECT
    t.modalidad,
    f.accede_soporte,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)   AS indice_bienestar,
    ROUND(AVG(f.productividad), 2)       AS productividad_promedio,
    ROUND(SUM(CASE WHEN f.productividad >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS productividad_sostenida_pct
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
GROUP BY t.modalidad, f.accede_soporte
ORDER BY t.modalidad, f.accede_soporte;
```

**Insight que aporta**: Justifica inversión en programas de soporte mental si se correlaciona con productividad.

---

### PA-05: ¿Existe un umbral de horas trabajadas donde el deterioro se acelera?

**Hipótesis**: Más de 45 horas semanales correlaciona con deterioro acelerado del bienestar.

**Query SQL**:
```sql
-- Análisis de bienestar por tramo de horas trabajadas
WITH tramos_horas AS (
    SELECT
        f.*,
        t.modalidad,
        t.horas_semanales,
        CASE
            WHEN t.horas_semanales <= 35 THEN '1. <=35h (Part-time)'
            WHEN t.horas_semanales <= 40 THEN '2. 36-40h (Full-time normal)'
            WHEN t.horas_semanales <= 45 THEN '3. 41-45h (Full-time más)'
            WHEN t.horas_semanales <= 50 THEN '4. 46-50h (Sobre tiempo)'
            ELSE '5. >50h (Exceso)'
        END AS tramo_horas
    FROM fact_bienestar_laboral f
    INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
    WHERE t.horas_semanales IS NOT NULL
)
SELECT
    modalidad,
    tramo_horas,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(indice_bienestar), 2)      AS indice_bienestar,
    ROUND(AVG(nivel_estres), 2)          AS estres_promedio,
    ROUND(AVG(productividad), 2)         AS productividad_promedio
FROM tramos_horas
GROUP BY modalidad, tramo_horas
ORDER BY modalidad,
    CASE WHEN tramo_horas LIKE '1.%' THEN 1
         WHEN tramo_horas LIKE '2.%' THEN 2
         WHEN tramo_horas LIKE '3.%' THEN 3
         WHEN tramo_horas LIKE '4.%' THEN 4
         ELSE 5 END;
```

**Insight que aporta**: Define políticas de horas extras basadas en datos de salud mental.

---

### PA-06 (NUEVA): ¿La edad es un factor de riesgo para el deterioro bajo trabajo remoto?

**Hipótesis**: Trabajadores mayores de 45 años reportan peor balance vida-trabajo en entornos remotos.

**Query SQL**:
```sql
-- Bienestar por rango etario y modalidad
SELECT
    t.modalidad,
    d.rango_etario,
    COUNT(*)                             AS n_empleados,
    ROUND(AVG(f.indice_bienestar), 2)   AS indice_bienestar,
    ROUND(AVG(f.balance_work_life), 2)  AS balance_promedio,
    ROUND(AVG(f.nivel_estres), 2)       AS estres_promedio
FROM fact_bienestar_laboral f
INNER JOIN dim_trabajo t ON f.id_trabajo = t.id_trabajo
INNER JOIN dim_demografia d ON f.id_demografia = d.id_demografia
GROUP BY t.modalidad, d.rango_etario
HAVING COUNT(*) >= 10
ORDER BY t.modalidad, d.rango_etario;
```

**Insight que aporta**: Identifica grupos etarios vulnerables para programas focalizados.

---

## 📈 Dashboard Power BI — KPIs Sugeridos

### Tarjetas de resumen (1 por KPI)
1. **Índice de Bienestar Promedio** — valor actual vs meta (≥7)
2. **Tasa de Alto Estrés** — valor actual vs meta (<20%)
3. **Cobertura de Soporte** — valor actual vs meta (≥80%)
4. **Productividad Sostenida** — valor actual vs meta (≥60%)

### Visualizaciones recomendadas
| Visual | Datos | Propósito |
|--------|-------|-----------|
| Gráfico de barras | Estrés promedio por modalidad | Comparar modalidades |
| Gráfico de líneas | Bienestar por rango etario | Ver tendencias |
| Heatmap | Estrés por industria × género | Identificar intersecciones |
| Scatter plot | Horas trabajadas vs Índice bienestar | Ver correlación |
| Tabla | Top 10 industrias con peor bienestar | Priorizar acciones |

### Colores sugeridos
- **Fondo**: #13161f
- **Acento principal**: #5ee7b0 (verde menta — positivo)
- **Acento secundario**: #7b8cff (azul índigo)
- **Alerta**: #ff7eb3 (rosado — indicadores en riesgo)
- **Advertencia**: #ffd166 (amarillo)
- **Texto principal**: #e8ecf4
- **Texto secundario**: #7a84a0

---

## 📁 Archivos SQL

| Archivo | Descripción |
|---------|-------------|
| `database/seeds/02_views_kpis.sql` | 3 vistas: `v_kpis_globales`, `v_kpis_por_modalidad`, `v_brecha_genero` |
| `database/seeds/03_consultas_analiticas.sql` | PA-01 a PA-06: Todas las preguntas analíticas |

---

## 📈 Resultados de KPIs vs Metas (2026-03-26)

| KPI | Resultado Global | Meta | Estado |
|-----|------------------|------|--------|
| Índice de Bienestar | 5.99 / 10 | ≥ 7.0 | ❌ Por debajo |
| Tasa de Alto Estrés | 33.7% | < 20% | ❌ Por encima |
| Cobertura Soporte | 48.9% | ≥ 80% | ❌ Por debajo |
| Productividad Sostenida | 0.0% | ≥ 60% | ❌ Sin datos válidos |

> **Nota**: La productividad sostenida muestra 0% porque los datos no alcanzan el umbral de >= 4 en la escala 1-5.

---

## 🔧 Mantenimiento

- **Frecuencia de actualización**: Mensual
- **Responsable**: Equipo de People Analytics
- **Última actualización**: 2026-03-26

## ✅ Checklist de Implementación

- [ ] Crear vista `v_kpis_globales` en PostgreSQL
- [ ] Crear vista `v_brecha_genero` para dashboard
- [ ] Crear scripts SQL en `database/seeds/`
- [ ] Implementar medidas DAX en Power BI
- [ ] Crear dashboard con las 4 tarjetas de KPI
- [ ] Agregar gráficos de comparación por modalidad
