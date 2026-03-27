-- ================================================================================
-- SCRIPT DDL — STAR SCHEMA SALUD MENTAL TRABAJO REMOTO
-- ================================================================================
-- Crea el modelo dimensional completo: dimensiones, tabla de hechos,
-- índices optimizados y constraints de integridad referencial.
--
-- Base de datos: mental_health_db (PostgreSQL 15)
-- Esquema:       public
--
-- Autor: Gerardo Suárez T. (gtsuarez-analytics)
-- Proyecto: Salud Mental en el Trabajo Remoto
-- ================================================================================

-- ── Eliminar tablas si existen (orden inverso por FK) ────────────
DROP TABLE IF EXISTS fact_bienestar_laboral CASCADE;
DROP TABLE IF EXISTS dim_soporte CASCADE;
DROP TABLE IF EXISTS dim_demografia CASCADE;
DROP TABLE IF EXISTS dim_trabajo CASCADE;
DROP TABLE IF EXISTS dim_tiempo CASCADE;

-- ── DIMENSIÓN: TIEMPO ────────────────────────────────────────────
CREATE TABLE dim_tiempo (
    id_tiempo      SERIAL PRIMARY KEY,
    anno           INTEGER NOT NULL,
    mes            INTEGER NOT NULL CHECK (mes BETWEEN 1 AND 12),
    trimestre      INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semestre       INTEGER NOT NULL CHECK (semestre BETWEEN 1 AND 2),
    UNIQUE (anno, mes)
);

-- ── DIMENSIÓN: TRABAJO ────────────────────────────────────────────
CREATE TABLE dim_trabajo (
    id_trabajo     SERIAL PRIMARY KEY,
    modalidad      VARCHAR(20) NOT NULL,
    industria      VARCHAR(100),
    rol            VARCHAR(100),
    empresa_tamano VARCHAR(20),
    horas_semanales INTEGER,
    UNIQUE (modalidad, industria, rol, empresa_tamano, horas_semanales)
);

-- ── DIMENSIÓN: DEMOGRAFÍA ────────────────────────────────────────
CREATE TABLE dim_demografia (
    id_demografia  SERIAL PRIMARY KEY,
    genero         VARCHAR(20) NOT NULL,
    rango_etario   VARCHAR(20) NOT NULL,
    region         VARCHAR(50),
    pais           VARCHAR(50),
    UNIQUE (genero, rango_etario, region, pais)
);

-- ── DIMENSIÓN: SOPORTE MENTAL ────────────────────────────────────
CREATE TABLE dim_soporte (
    id_soporte     SERIAL PRIMARY KEY,
    tipo_soporte   VARCHAR(50) NOT NULL,
    disponible     BOOLEAN NOT NULL,
    UNIQUE (tipo_soporte, disponible)
);

-- ── TABLA DE HECHOS: BIENESTAR LABORAL ───────────────────────────
CREATE TABLE fact_bienestar_laboral (
    id_empleado         VARCHAR(64) NOT NULL,
    id_trabajo          INTEGER NOT NULL REFERENCES dim_trabajo(id_trabajo),
    id_demografia       INTEGER NOT NULL REFERENCES dim_demografia(id_demografia),
    id_tiempo           INTEGER NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_soporte          INTEGER NOT NULL REFERENCES dim_soporte(id_soporte),
    nivel_estres        INTEGER NOT NULL CHECK (nivel_estres BETWEEN 1 AND 5),
    balance_work_life   INTEGER NOT NULL CHECK (balance_work_life BETWEEN 1 AND 5),
    productividad       INTEGER NOT NULL CHECK (productividad BETWEEN 1 AND 5),
    indice_bienestar    FLOAT NOT NULL,
    accede_soporte      BOOLEAN NOT NULL,
    satisfaccion_laboral INTEGER CHECK (satisfaccion_laboral BETWEEN 1 AND 5),
    fecha_registro      DATE
);

-- ── ÍNDICES (optimización de queries analíticas) ─────────────────
CREATE INDEX idx_fact_id_trabajo    ON fact_bienestar_laboral(id_trabajo);
CREATE INDEX idx_fact_id_demografia ON fact_bienestar_laboral(id_demografia);
CREATE INDEX idx_fact_id_tiempo     ON fact_bienestar_laboral(id_tiempo);
CREATE INDEX idx_fact_id_soporte    ON fact_bienestar_laboral(id_soporte);
CREATE INDEX idx_fact_estres        ON fact_bienestar_laboral(nivel_estres);
CREATE INDEX idx_fact_modalidad     ON fact_bienestar_laboral(id_trabajo, nivel_estres);
CREATE INDEX idx_trabajo_modalidad  ON dim_trabajo(modalidad);
CREATE INDEX idx_dem_genero         ON dim_demografia(genero);

-- ── COMENTARIOS DE COLUMNAS ──────────────────────────────────────
COMMENT ON TABLE  fact_bienestar_laboral IS 'Tabla de hechos principal: una fila por observación de bienestar laboral';
COMMENT ON COLUMN fact_bienestar_laboral.indice_bienestar IS 'KPI compuesto: (6-nivel_estres + balance + productividad) / 3 * 2, escala 1-10';
COMMENT ON COLUMN fact_bienestar_laboral.id_empleado     IS 'Identificador anonimizado del empleado (hash SHA-256 del índice original)';
