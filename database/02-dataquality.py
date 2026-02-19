# informe_limpieza.py
# Genera un informe de validación de calidad de datos en las 3 tablas
# Amigable para usuarios no informáticos
# Ejecutar después de la carga para detectar problemas

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Configuración (ajusta según tu entorno)
DB_URL = "postgresql://migx_user:migx_password@localhost:5434/clinical_db"
OUTPUT_FILE = "informe_limpieza_datos.txt"

engine = create_engine(DB_URL)

def generar_informe():
    """Genera informe de calidad de datos amigable para usuarios no técnicos"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    lineas = [
        "╔" + "═"*78 + "╗",
        "║" + " INFORME DE CALIDAD DE DATOS - ENSAYOS CLÍNICOS ".center(78) + "║",
        "║" + f" Generado: {timestamp}".ljust(78) + "║",
        "╚" + "═"*78 + "╝",
        ""
    ]

    estado_general = True  # Bandera para estado global

    # 1. UNICIDAD EN CONDICIONES
    lineas.append("┌─ VALIDACIÓN 1: Nombres de Condiciones Únicos")
    lineas.append("└─ Propósito: Evitar duplicados de condiciones comunes")
    try:
        df = pd.read_sql("""
            SELECT condition_name, COUNT(*) AS conteo
            FROM conditions
            GROUP BY condition_name
            HAVING COUNT(*) > 1
            ORDER BY conteo DESC;
        """, engine)
        
        if len(df) == 0:
            lineas.append("   ✓ ESTADO: OK - No hay condiciones duplicadas")
            lineas.append("   Métrica: 0 duplicados")
        else:
            estado_general = False
            lineas.append("   ✗ ESTADO: PROBLEMAS DETECTADOS")
            lineas.append(f"   Métrica: {len(df)} condiciones duplicadas")
            lineas.append("   Acción: Investiga el CSV fuente para corregir variaciones (ej. 'diabetes' vs 'Diabetes')")
            for _, row in df.iterrows():
                lineas.append(f"     • '{row['condition_name']}': {row['conteo']} registros")
    except Exception as e:
        lineas.append(f"   ✗ ERROR: {str(e)}")
        estado_general = False
    
    lineas.append("")

    # 2. INTEGRIDAD REFERENCIAL
    lineas.append("┌─ VALIDACIÓN 2: Integridad de Referencias (condiciones y estudios)")
    lineas.append("└─ Propósito: Asegurar que todas las relaciones sean válidas")
    
    try:
        # Conditions huérfanos
        df_cond = pd.read_sql("""
            SELECT COUNT(DISTINCT sc.condition_id) as huerfanos
            FROM study_conditions sc
            LEFT JOIN conditions c ON sc.condition_id = c.id
            WHERE c.id IS NULL;
        """, engine)
        huerfanos_cond = df_cond['huerfanos'].iloc[0]
        
        # Studies huérfanos
        df_stud = pd.read_sql("""
            SELECT COUNT(DISTINCT sc.study_key) as huerfanos
            FROM study_conditions sc
            LEFT JOIN studies s ON sc.study_key = s.study_key
            WHERE s.study_key IS NULL;
        """, engine)
        huerfanos_stud = df_stud['huerfanos'].iloc[0]
        
        total_problemas = huerfanos_cond + huerfanos_stud
        
        if total_problemas == 0:
            lineas.append("   ✓ ESTADO: OK - Todas las referencias son válidas")
            lineas.append("   Métrica: 0 referencias rotas")
        else:
            estado_general = False
            lineas.append("   ✗ ESTADO: PROBLEMAS DETECTADOS")
            lineas.append(f"   Métrica: {total_problemas} referencias inválidas")
            if huerfanos_cond > 0:
                lineas.append(f"     • {huerfanos_cond} ID(s) de condición que no existen en tabla conditions")
            if huerfanos_stud > 0:
                lineas.append(f"     • {huerfanos_stud} clave(s) de estudio que no existen en tabla studies")
            lineas.append("   Acción: Revisa el CSV fuente para inconsistencias en carga secuencial")
    except Exception as e:
        lineas.append(f"   ✗ ERROR: {str(e)}")
        estado_general = False
    
    lineas.append("")

    # 3. COMPLETITUD EN CAMPOS CLAVE
    lineas.append("┌─ VALIDACIÓN 3: Campos Obligatorios Completos")
    lineas.append("└─ Propósito: Asegurar que no falten datos esenciales")
    
    try:
        df = pd.read_sql("""
            SELECT 
                COUNT(*) AS total,
                COUNT(CASE WHEN brief_title IS NULL OR brief_title = '' THEN 1 END) AS titulos_vacios,
                COUNT(CASE WHEN org_name IS NULL OR org_name = '' THEN 1 END) AS orgs_vacios,
                COUNT(CASE WHEN overall_status IS NULL THEN 1 END) AS statuses_vacios,
                COUNT(CASE WHEN start_date IS NULL THEN 1 END) AS fechas_vacias
            FROM studies;
        """, engine)
        
        total = df['total'].iloc[0]
        titulos = df['titulos_vacios'].iloc[0]
        orgs = df['orgs_vacios'].iloc[0]
        statuses = df['statuses_vacios'].iloc[0]
        fechas = df['fechas_vacias'].iloc[0]
        
        total_vacios = titulos + orgs + statuses + fechas
        
        if total_vacios == 0:
            lineas.append("   ✓ ESTADO: OK - Todos los campos obligatorios están completos")
            lineas.append(f"   Métrica: {total} estudios, 0 campos vacíos")
        else:
            estado_general = False
            lineas.append("   ✗ ESTADO: CAMPOS VACÍOS DETECTADOS")
            lineas.append(f"   Métrica: {total_vacios} campos vacíos de {total * 4} total")
            lineas.append(f"     • Títulos vacíos: {titulos}/{total}")
            lineas.append(f"     • Organizaciones vacías: {orgs}/{total}")
            lineas.append(f"     • Estados vacíos: {statuses}/{total}")
            lineas.append(f"     • Fechas de inicio vacías: {fechas}/{total}")
            lineas.append("   Acción: Investiga el CSV fuente para imputación o filtrado de registros")
    except Exception as e:
        lineas.append(f"   ✗ ERROR: {str(e)}")
        estado_general = False
    
    lineas.append("")

    # 4. CONSISTENCIA EN FECHAS
    lineas.append("┌─ VALIDACIÓN 4: Fechas de Inicio Lógicas")
    lineas.append("└─ Propósito: Detectar fechas imposibles o inconsistentes")
    
    try:
        df = pd.read_sql("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN start_date IS NULL THEN 1 END) as nulos,
                COUNT(CASE WHEN start_date > CURRENT_DATE THEN 1 END) as futuras,
                COUNT(CASE WHEN start_date >= '2026-01-01' THEN 1 END) as año_2026_posterior
            FROM studies;
        """, engine)
        
        total = df['total'].iloc[0]
        nulos = df['nulos'].iloc[0]
        futuras = df['futuras'].iloc[0]
        año_posterior = df['año_2026_posterior'].iloc[0]
        
        problemas_fecha = nulos + futuras
        
        if problemas_fecha == 0:
            lineas.append("   ✓ ESTADO: OK - Todas las fechas son válidas")
            lineas.append(f"   Métrica: {total} estudios, 0 fechas problemáticas")
        else:
            estado_general = False
            lineas.append("   ✗ ESTADO: FECHAS PROBLEMÁTICAS")
            lineas.append(f"   Métrica: {problemas_fecha}/{total} estudios con fechas sospechosas")
            if nulos > 0:
                lineas.append(f"     • Fechas ausentes (NULL): {nulos}")
            if futuras > 0:
                lineas.append(f"     • Fechas en el futuro (imposibles): {futuras}")
            lineas.append("   Acción: Revisa registros con fechas problemáticas en el CSV fuente")
    except Exception as e:
        lineas.append(f"   ✗ ERROR: {str(e)}")
        estado_general = False
    
    lineas.append("")

    # 5. OUTLIERS EN NÚMERO DE CONDICIONES
    lineas.append("┌─ VALIDACIÓN 5: Número de Condiciones por Estudio")
    lineas.append("└─ Propósito: Detectar estudios sin condiciones o con demasiadas")
    
    try:
        df = pd.read_sql("""
            SELECT 
                COUNT(CASE WHEN num_cond = 0 THEN 1 END) as sin_condiciones,
                COUNT(CASE WHEN num_cond > 10 THEN 1 END) as muchas_condiciones,
                COUNT(*) as total_estudios
            FROM (
                SELECT COUNT(condition_id) as num_cond
                FROM study_conditions
                GROUP BY study_key
            ) subq;
        """, engine)
        
        sin_cond = df['sin_condiciones'].iloc[0]
        muchas = df['muchas_condiciones'].iloc[0]
        total_est = df['total_estudios'].iloc[0]
        
        problemas = sin_cond + muchas
        
        if problemas == 0:
            lineas.append("   ✓ ESTADO: OK - Distribución normal de condiciones por estudio")
            lineas.append(f"   Métrica: {total_est} estudios, todos con 1-10 condiciones")
        else:
            estado_general = False
            lineas.append("   ✗ ESTADO: OUTLIERS DETECTADOS")
            lineas.append(f"   Métrica: {problemas}/{total_est} estudios con distribución anómala")
            if sin_cond > 0:
                lineas.append(f"     • Estudios sin condiciones: {sin_cond}")
            if muchas > 0:
                lineas.append(f"     • Estudios con >10 condiciones: {muchas}")
            lineas.append("   Acción: Investiga si hay errores en el split de valores múltiples (CSV)")
    except Exception as e:
        lineas.append(f"   ✗ ERROR: {str(e)}")
        estado_general = False
    
    lineas.append("")

    # 6. DUPLICADOS
    lineas.append("┌─ VALIDACIÓN 6: Duplicados por Título + Organización")
    lineas.append("└─ Propósito: Identificar estudios repetidos o parcialmente duplicados")
    
    try:
        df = pd.read_sql("""
            SELECT COUNT(*) as num_grupos_duplicados
            FROM (
                SELECT brief_title, org_name, COUNT(*) as conteo
                FROM studies
                GROUP BY brief_title, org_name
                HAVING COUNT(*) > 1
            ) subq;
        """, engine)
        
        num_grupos = df['num_grupos_duplicados'].iloc[0]
        
        if num_grupos == 0:
            lineas.append("   ✓ ESTADO: OK - No hay duplicados detectados")
            lineas.append("   Métrica: 0 grupos duplicados")
        else:
            estado_general = False
            lineas.append("   ✗ ESTADO: DUPLICADOS PARCIALES DETECTADOS")
            lineas.append(f"   Métrica: {num_grupos} grupos de estudios duplicados")
            
            # Mostrar los principales
            df_detalles = pd.read_sql("""
                SELECT 
                    brief_title,
                    org_name,
                    COUNT(*) AS num_registros,
                    COUNT(DISTINCT start_date) as fechas_distintas
                FROM studies
                GROUP BY brief_title, org_name
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 5;
            """, engine)
            
            for _, row in df_detalles.iterrows():
                lineas.append(f"     • '{row['brief_title'][:50]}...' / '{row['org_name'][:30]}...'")
                lineas.append(f"       → {row['num_registros']} registros ({row['fechas_distintas']} fechas)")
            
            lineas.append("   Acción: Mantén registros más recientes/antiguos según criterio del equipo")
    except Exception as e:
        lineas.append(f"   ✗ ERROR: {str(e)}")
        estado_general = False
    
    lineas.append("")
    lineas.append("")

    # RESUMEN FINAL
    lineas.append("╔" + "═"*78 + "╗")
    if estado_general:
        lineas.append("║" + " ✓ ESTADO GENERAL: DATOS CON BUENA CALIDAD ".center(78) + "║")
        lineas.append("║" + " Los datos están listos para analítica ".center(78) + "║")
    else:
        lineas.append("║" + " ✗ ESTADO GENERAL: REVISAR PROBLEMAS DETECTADOS ".center(78) + "║")
        lineas.append("║" + " Investiga el CSV fuente para correcciones ".center(78) + "║")
    lineas.append("╚" + "═"*78 + "╝")

    # Guardar informe
    contenido = "\n".join(lineas)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(contenido)

    print(contenido)
    print(f"\n✓ Informe guardado en: {OUTPUT_FILE}")

if __name__ == "__main__":
    generar_informe()