-- =============================================================================
-- queries.sql
-- Queries analíticas para el pipeline de ensayos clínicos
-- Responde preguntas clave del desafío
-- =============================================================================

-- =============================================================================
-- 1. ¿CUÁNTOS ENSAYOS HAY POR TIPO DE ESTUDIO Y FASE?
-- =============================================================================

SELECT 
    study_type,
    COALESCE(phase, 'NO_ESPECIFICADO') AS phase,
    COUNT(*) AS numero_estudios,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje,
    COUNT(CASE WHEN overall_status = 'COMPLETED' THEN 1 END) AS completados,
    ROUND(100.0 * COUNT(CASE WHEN overall_status = 'COMPLETED' THEN 1 END) / 
          COUNT(*), 2) AS tasa_completacion
FROM studies
GROUP BY study_type, phase
ORDER BY numero_estudios DESC;

-- =============================================================================
-- 2. ¿CUÁLES SON LAS CONDICIONES MÁS COMÚNMENTE ESTUDIADAS?
-- =============================================================================

SELECT 
    c.condition_name,
    COUNT(DISTINCT sc.study_key) AS numero_estudios,
    ROUND(100.0 * COUNT(DISTINCT sc.study_key) / 
          (SELECT COUNT(DISTINCT study_key) FROM studies), 2) AS porcentaje_cobertura,
    COUNT(CASE WHEN s.overall_status = 'COMPLETED' 
              THEN 1 END) AS estudios_completados
FROM conditions c
LEFT JOIN study_conditions sc ON c.id = sc.condition_id
LEFT JOIN studies s ON sc.study_key = s.study_key
GROUP BY c.condition_name
HAVING COUNT(DISTINCT sc.study_key) > 0
ORDER BY numero_estudios DESC
LIMIT 20;  -- Top 20 condiciones

-- =============================================================================
-- 3. DISTRIBUCIÓN DE ESTUDIOS POR STATUS
-- =============================================================================

SELECT 
    overall_status,
    COUNT(*) AS numero_estudios,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje,
    COUNT(CASE WHEN start_date IS NOT NULL THEN 1 END) AS con_fecha_inicio,
    ROUND(AVG(CASE WHEN start_date IS NOT NULL THEN 
              CAST((CURRENT_DATE - start_date) AS numeric) ELSE NULL END), 1) 
        AS dias_promedio_desde_inicio
FROM studies
GROUP BY overall_status
ORDER BY numero_estudios DESC;

-- =============================================================================
-- 4. ANÁLISIS TEMPORAL: DISTRIBUCIÓN POR AÑO DE INICIO
-- =============================================================================

SELECT 
    EXTRACT(YEAR FROM start_date)::INTEGER AS anio,
    COUNT(*) AS numero_estudios,
    COUNT(CASE WHEN overall_status = 'COMPLETED' THEN 1 END) AS completados,
    COUNT(CASE WHEN overall_status = 'RECRUITING' THEN 1 END) AS en_reclutamiento,
    COUNT(CASE WHEN overall_status = 'SUSPENDED' THEN 1 END) AS suspendidos,
    ROUND(100.0 * COUNT(CASE WHEN overall_status = 'COMPLETED' THEN 1 END) / 
          COUNT(*), 2) AS tasa_completacion
FROM studies
WHERE start_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM start_date)
ORDER BY anio DESC;

-- =============================================================================
-- 5. ESTUDIOS POR ORGANIZACIÓN (TOP 10)
-- =============================================================================

SELECT 
    org_name,
    COUNT(*) AS numero_estudios,
    COUNT(DISTINCT sc.condition_id) AS num_condiciones_unicas,
    COUNT(CASE WHEN overall_status = 'COMPLETED' THEN 1 END) AS completados,
    ROUND(100.0 * COUNT(CASE WHEN overall_status = 'COMPLETED' THEN 1 END) / 
          COUNT(*), 2) AS tasa_completacion
FROM studies s
LEFT JOIN study_conditions sc ON s.study_key = sc.study_key
GROUP BY org_name
HAVING COUNT(*) >= 2  -- Al menos 2 estudios
ORDER BY numero_estudios DESC
LIMIT 10;

-- =============================================================================
-- 6. NÚMERO DE CONDICIONES POR ESTUDIO (ANÁLISIS DE DISTRIBUCIÓN)
-- =============================================================================

SELECT 
    num_condiciones,
    COUNT(*) AS numero_estudios,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
FROM (
    SELECT 
        s.study_key,
        COUNT(sc.condition_id) AS num_condiciones
    FROM studies s
    LEFT JOIN study_conditions sc ON s.study_key = sc.study_key
    GROUP BY s.study_key
) subq
GROUP BY num_condiciones
ORDER BY num_condiciones;

-- =============================================================================
-- 7. STUDIES CON MEJORES/PEORES RESULTADOS
-- =============================================================================

-- Estudios más antiguos (iniciados hace más tiempo)
SELECT 
    s.study_key AS study_key,
    s.brief_title,
    s.org_name,
    s.start_date,
    CAST((CURRENT_DATE - s.start_date) AS numeric) AS dias_duracion,
    s.overall_status,
    COUNT(DISTINCT sc.condition_id) AS numero_condiciones
FROM studies s
LEFT JOIN study_conditions sc ON s.study_key = sc.study_key
WHERE s.start_date IS NOT NULL
GROUP BY s.study_key, s.brief_title, s.org_name, s.start_date, s.overall_status
ORDER BY s.start_date ASC
LIMIT 10;

-- =============================================================================
-- 8. CALIDAD DE DATOS: CAMPOS CRÍTICOS
-- =============================================================================

SELECT 
    COUNT(*) AS total_estudios,
    COUNT(CASE WHEN brief_title IS NULL OR brief_title = '' THEN 1 END) AS titulos_vacios,
    COUNT(CASE WHEN org_name IS NULL OR org_name = '' THEN 1 END) AS orgs_vacias,
    COUNT(CASE WHEN overall_status IS NULL THEN 1 END) AS status_vacios,
    COUNT(CASE WHEN start_date IS NULL THEN 1 END) AS fechas_vacias,
    COUNT(CASE WHEN phase IS NULL THEN 1 END) AS fases_vacias,
    ROUND(100.0 * COUNT(CASE WHEN brief_title IS NOT NULL AND 
                            org_name IS NOT NULL AND 
                            overall_status IS NOT NULL AND 
                            start_date IS NOT NULL 
                       THEN 1 END) / COUNT(*), 2) AS completitud_promedio
FROM studies;

-- =============================================================================
-- 9. RESUMEN EJECUTIVO
-- =============================================================================

SELECT 
    'Total de Estudios' AS metrica,
    COUNT(*) AS valor
FROM studies

UNION ALL

SELECT 
    'Estudios Completados',
    COUNT(CASE WHEN overall_status = 'COMPLETED' THEN 1 END)
FROM studies

UNION ALL

SELECT 
    'Estudios en Reclutamiento',
    COUNT(CASE WHEN overall_status = 'RECRUITING' THEN 1 END)
FROM studies

UNION ALL

SELECT 
    'Condiciones Únicas',
    COUNT(DISTINCT id)
FROM conditions

UNION ALL

SELECT 
    'Estudios sin Condiciones Asignadas',
    COUNT(DISTINCT s.study_key)
FROM studies s
LEFT JOIN study_conditions sc ON s.study_key = sc.study_key
WHERE sc.condition_id IS NULL

UNION ALL

SELECT 
    'Organizaciones Participantes',
    COUNT(DISTINCT org_name)
FROM studies

UNION ALL

SELECT 
    'Estudios con Fechas Futuras (Anomalía)',
    COUNT(CASE WHEN start_date > CURRENT_DATE THEN 1 END)
FROM studies;

-- =============================================================================
-- 10. VISTA: ESTUDIOS CON CONDICIONES AGREGADAS (ÚTIL PARA DASHBOARDS)
-- =============================================================================

-- Esta vista ya existe en 02-create.sql
-- CREATE VIEW public.v_studies_with_conditions AS
-- SELECT 
--     s.study_key,
--     s.brief_title,
--     s.overall_status,
--     s.phase,
--     s.study_type,
--     s.start_date,
--     string_agg(c.condition_name, ' | ') AS conditions_list
-- FROM public.studies s
-- LEFT JOIN public.study_conditions sc ON sc.study_key = s.study_key
-- LEFT JOIN public.conditions c ON c.id = sc.condition_id
-- GROUP BY s.study_key, s.brief_title, s.overall_status, s.phase, s.study_type, s.start_date;

-- =============================================================================
-- NOTES
-- =============================================================================
-- 
-- Estas queries responden las preguntas clave del desafío:
-- 
-- 1. ¿Cuántos ensayos hay por tipo de estudio y fase?
--    → Query 1 + Query 3 (distribución por status)
-- 
-- 2. ¿Cuáles son las condiciones más comúnmente estudiadas?
--    → Query 2 (Top 20 condiciones)
-- 
-- 3. ¿Qué intervenciones tienen las tasas de finalización más altas?
--    → Query 3 (tasa de finalización por status)
--    → Query 5 (tasa por organización)
--    → Nota: Dataset no incluye intervenciones separadas
-- 
-- 4. Distribución geográfica de los ensayos clínicos
--    → Query 5 (por organización, proxy geográfico)
--    → Nota: Dataset no incluye ubicación/país explícita
-- 
-- 5. Análisis temporal de la duración de los estudios
--    → Query 4 (por año de inicio)
--    → Query 7 (duración de estudios individuales)
--    → Query 8 (calidad de datos)
--
-- PARA EJECUTAR EN PSQL:
-- psql -U migx_user -d clinical_db -f analytics/queries.sql
--
-- PARA GUARDAR RESULTADOS EN CSV:
-- psql -U migx_user -d clinical_db -c "\COPY (SELECT ...) TO output.csv WITH CSV HEADER"
--
