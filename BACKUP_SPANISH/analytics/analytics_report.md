# Informe Analítico de Ensayos Clínicos

## 1. Volumen y Estado General

El dataset consolidado cuenta con **495.634 estudios**, gestionados por **28.081 organizaciones**. La mayoría de los ensayos se encuentran en estado `COMPLETED` (54,6%), seguidos por aquellos en fase de `RECRUITING` (14,3%). Se han identificado **108.142 condiciones clínicas únicas**, lo que refleja la alta diversidad del ecosistema médico analizado.

---

## 2. Condiciones y Complejidad Médica

**Top Condiciones.** Los estudios en pacientes "Healthy" (sanos) lideran el ranking con más de 10.000 ensayos. Le siguen condiciones de alta prevalencia como el cáncer de mama, obesidad y diabetes mellitus.

**Distribución de Condiciones.** La mayoría de los estudios (61%) se centran en una única condición. Sin embargo, existe un grupo minoritario de estudios con una carga clínica compleja, llegando a registrarse ensayos con más de 20 condiciones asociadas.

---

## 3. Análisis Temporal y de Rendimiento

**Evolución.** Se observa un pico de actividad entre 2020 y 2022, probablemente impulsado por la investigación post-pandemia.

**Tasa de Finalización.** Los años más antiguos muestran tasas de completación superiores al 80%, mientras que los años recientes (2023-2024) reflejan una tasa baja debido a que la mayoría de los estudios siguen en curso.

**Longevidad.** Se han detectado registros históricos con duraciones superiores a los 100 años (ej. estudios iniciados en 1916/1917), los cuales requieren una validación manual para descartar errores de registro.

---

## 4. Líderes del Sector (Top 10 Organizaciones)

El **National Cancer Institute (NCI)** encabeza la lista con más de 18.800 estudios, seguido por el **National Institutes of Health (NIH)**. Es destacable que organizaciones farmacéuticas como GlaxoSmithKline muestran una eficiencia de completación muy alta (87,6%), en comparación con centros académicos que suelen manejar estudios a más largo plazo.

---

## 5. Auditoría de Calidad de Datos (Campos Críticos)

El análisis de calidad revela áreas de mejora urgentes para la fiabilidad del modelo:

**Completitud.** La tasa de completitud promedio es del 55,7%.

**Fechas.** Existen 219.166 registros con fechas vacías (44% del total), lo que limita el análisis de tendencias históricas.

**Anomalías.** Se detectaron 32 estudios con fechas futuras (posteriores a la fecha actual), confirmando la necesidad de implementar reglas de validación en la capa de ingesta.