-- =============================================================================
-- create.sql
-- Esquema de base de datos para ensayos clínicos (dataset curt_trials.csv)
-- Versión 2 - Mejoras respecto a v1
-- =============================================================================

-- Razones principales de diseño:
-- 1. Usamos surrogate key estable (study_key VARCHAR(16)) generado con hash MD5
--    porque el CSV NO incluye NCT Number ni otro identificador natural único.
-- 2. Normalizamos 'conditions' en tabla separada con surrogate key (id) para
--    evitar duplicados y facilitar agregaciones / top condiciones más frecuentes.
-- 3. Añadimos más columnas relevantes del CSV para soportar las analíticas pedidas
--    (conteo por fase, duración de estudios, condiciones comunes, etc.)
-- 4. Tipos de datos más precisos + constraints básicas para calidad
-- 5. Índices en columnas frecuentes en WHERE/GROUP BY para rendimiento
-- =============================================================================

-- Limpiar si existe (útil para desarrollo / pruebas)
DROP TABLE IF EXISTS public.study_conditions CASCADE;
DROP TABLE IF EXISTS public.conditions CASCADE;
DROP TABLE IF EXISTS public.studies CASCADE;
DROP TABLE IF EXISTS study_conditions, conditions, studies CASCADE;

-- Tabla principal: estudios clínicos
CREATE TABLE public.studies (
    study_key       VARCHAR(16) PRIMARY KEY,            -- hash MD5 de campos únicos
    brief_title     TEXT NOT NULL,
    full_title      TEXT,
    org_name        TEXT NOT NULL,
    org_class       VARCHAR(50),
    responsible_party VARCHAR(100),
    overall_status  VARCHAR(50) NOT NULL,
    study_type      VARCHAR(50) NOT NULL,
    phase           VARCHAR(50),                        -- puede ser NA, PHASE1, etc.
    start_date      DATE,
    -- completion_date DATE,                             -- no visible en sample, pero común
    standard_age    TEXT,                               -- e.g. "ADULT OLDER_ADULT"
    primary_purpose VARCHAR(50),
    enrollment      INTEGER,                            -- si aparece en CSV completo
    -- sponsor         TEXT,                             -- a veces se deduce de org_name
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    --CONSTRAINT valid_status CHECK (overall_status IN (
    --    'COMPLETED', 'RECRUITING', 'NOT_YET_RECRUITING', 'ACTIVE_NOT_RECRUITING',
    --    'TERMINATED', 'WITHDRAWN', 'UNKNOWN', 'SUSPENDED'
    --))
    CONSTRAINT valid_status CHECK (overall_status IN (
        'RECRUITING',
        'NOT_YET_RECRUITING',
        'ACTIVE_NOT_RECRUITING',
        'ENROLLING_BY_INVITATION',
        'COMPLETED',
        'SUSPENDED',
        'TERMINATED',
        'WITHDRAWN',
        'UNKNOWN',
        'APPROVED_FOR_MARKETING',
        'WITHHELD',
        'AVAILABLE',                      -- ← clave aquí
        'NO_LONGER_AVAILABLE',
        'TEMPORARILY_NOT_AVAILABLE'
))
);

-- Tabla de condiciones únicas (normalización)
CREATE TABLE public.conditions (
    id              SERIAL PRIMARY KEY,
    condition_name  TEXT NOT NULL UNIQUE,               -- evita duplicados
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de relación muchos-a-muchos
CREATE TABLE public.study_conditions (
    study_key       VARCHAR(16) REFERENCES studies(study_key) ON DELETE CASCADE,
    condition_id    INTEGER REFERENCES conditions(id) ON DELETE CASCADE,
    PRIMARY KEY (study_key, condition_id)
);

-- Índices para mejorar rendimiento en consultas analíticas frecuentes
CREATE INDEX idx_studies_status        ON studies(overall_status);
CREATE INDEX idx_studies_phase         ON studies(phase);
CREATE INDEX idx_studies_study_type    ON studies(study_type);
CREATE INDEX idx_studies_start_date    ON studies(start_date);
CREATE INDEX idx_conditions_name       ON conditions(condition_name);

-- Vista de ejemplo útil para analítica rápida (opcional pero recomendado)
CREATE VIEW public.v_studies_with_conditions AS
SELECT 
    s.study_key,
    s.brief_title,
    s.overall_status,
    s.phase,
    s.study_type,
    s.start_date,
    string_agg(c.condition_name, ' | ') AS conditions_list
FROM public.studies s
LEFT JOIN public.study_conditions sc ON sc.study_key = s.study_key
LEFT JOIN public.conditions c ON c.id = sc.condition_id
GROUP BY s.study_key, s.brief_title, s.overall_status, s.phase, s.study_type, s.start_date;

-- Comentario: puedes crear más vistas o materialized views cuando tengas
-- las analíticas concretas definidas (top 10 condiciones, ensayos por fase, etc.)




