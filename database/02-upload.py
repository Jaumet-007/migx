# =============================================================================
# upload.py
# ETL básico: carga dataset de ensayos clínicos (CSV) → PostgreSQL
# Versión: 2026-02-18 (con manejo opcional de estados inválidos)
#
# Cambios clave respecto a versión anterior:
# - Normalización de overall_status antes de insertar
# - Logging de advertencia + conteo cuando hay valores inesperados
# - Mapeo suave de estados raros a categorías cercanas
# - Opción comentada para filtrar filas inválidas (si se desea ser estricto)
# =============================================================================

import pandas as pd
import hashlib
import re
import logging
from sqlalchemy import create_engine, text

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────

CSV_PATH = r"G:\migx\dataset_csv\clin_trials.csv"           # ← AJUSTA TU RUTA
DB_URL   = "postgresql://migx_user:migx_password@localhost:5434/clinical_db"

# Lista de estados válidos (alineada con ClinicalTrials.gov + tu dataset)
VALID_STATUSES = {
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
    'TEMPORARILY_NOT_AVAILABLE',
    'NO_LONGER_AVAILABLE'
}

# Mapeo para normalizar estados raros (opcional)
STATUS_MAPPING = {
    'ENROLLING_BY_INVITATION': 'RECRUITING',    # muy similar
    'WITHHELD':                'UNKNOWN',
    'TEMPORARILY_NOT_AVAILABLE': 'SUSPENDED',
    # Añade más según veas en tu dataset
}


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES
# ──────────────────────────────────────────────────────────────────────────────

def generate_study_key(row) -> str:
    """Clave única determinística basada en campos estables"""
    unique_str = (
        f"{row.get('brief_title', '')}|"
        f"{row.get('full_title', '')}|"
        f"{row.get('organization_full_name', '')}|"
        f"{row.get('start_date', '')}"
    )
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()[:16]


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y mapea nombres de columnas del CSV a los esperados en BD"""
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(r'[^a-z0-9_]', '_', regex=True)
    )

    mapping = {
        'organization_full_name': 'org_name',
        'organization_class':     'org_class',
        'overall_status':         'overall_status',
        'study_type':             'study_type',
        'phase':                  'phase',
        'start_date':             'start_date',
        'standard_age':           'standard_age',
        'primary_purpose':        'primary_purpose',
        'brief_title':            'brief_title',
        'full_title':             'full_title',
        'responsible_party':      'responsible_party',
    }
    return df.rename(columns=mapping)


def extract_conditions(df: pd.DataFrame) -> pd.DataFrame:
    """Extrae y limpia condiciones (maneja coma y pipe)"""
    records = []
    if 'conditions' not in df.columns:
        logging.warning("Columna 'conditions' no encontrada")
        return pd.DataFrame()

    for _, row in df.iterrows():
        if pd.isna(row['conditions']) or not str(row['conditions']).strip():
            continue
        raw = re.split(r'\s*[,\|]\s*', str(row['conditions']))
        cleaned = {c.strip().lower() for c in raw if c.strip() and len(c.strip()) >= 3}
        for cond in cleaned:
            records.append({
                'study_key': row['study_key'],
                'condition_name': cond
            })
    return pd.DataFrame(records)


def normalize_statuses(studies: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza / mapea estados de overall_status.
    Registra advertencia si hay valores inesperados.
    """
    if 'overall_status' not in studies.columns:
        return studies

    # Aplicar mapeo suave
    studies['overall_status'] = studies['overall_status'].replace(STATUS_MAPPING)

    # Detectar valores no esperados
    unexpected = studies[~studies['overall_status'].isin(VALID_STATUSES)]
    if not unexpected.empty:
        logging.warning(
            f"Encontrados {len(unexpected)} filas con overall_status no esperado:\n"
            f"{unexpected['overall_status'].value_counts().to_string()}"
        )
        # Opcional: filtrar filas inválidas (descomenta si quieres ser estricto)
        # studies = studies[studies['overall_status'].isin(VALID_STATUSES)]
        # logging.info(f"Filas restantes tras filtrado: {len(studies)}")

    return studies


# ──────────────────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────

def cargar_datos():
    logging.info("Iniciando carga de datos...")

    # 1. Leer CSV
    try:
        df = pd.read_csv(CSV_PATH, dtype=str, low_memory=False)
        logging.info(f"CSV leído → {len(df):,} filas")
    except Exception as e:
        logging.error(f"Error al leer CSV: {e}")
        return

    # 2. Normalizar columnas + generar clave
    df = normalize_column_names(df)
    df['study_key'] = df.apply(generate_study_key, axis=1)
    df = df.drop_duplicates(subset='study_key', keep='first')
    logging.info(f"Filas únicas tras deduplicación: {len(df):,}")

    # 3. Preparar tabla studies
    target_cols = [
        'study_key', 'brief_title', 'full_title', 'org_name', 'org_class',
        'responsible_party', 'overall_status', 'study_type', 'phase',
        'start_date', 'standard_age', 'primary_purpose'
    ]
    existing_cols = [c for c in target_cols if c in df.columns]
    studies = df[existing_cols].copy()

    # Conversión de tipos
    if 'start_date' in studies.columns:
        studies['start_date'] = pd.to_datetime(studies['start_date'], errors='coerce')

    # 4. Normalizar estados (parte opcional activada)
    studies = normalize_statuses(studies)

    # 5. Procesar condiciones
    cond_df = extract_conditions(df)

    # 6. Cargar a PostgreSQL
    engine = create_engine(DB_URL)
    try:
        with engine.begin() as conn:
            # Limpiar (solo desarrollo)
            conn.execute(text("TRUNCATE TABLE study_conditions, conditions, studies RESTART IDENTITY CASCADE;"))

            # Condiciones únicas
            if not cond_df.empty:
                unique_cond = cond_df[['condition_name']].drop_duplicates()
                unique_cond.to_sql('conditions', conn, if_exists='append', index=False)
                cond_map = pd.read_sql("SELECT id, condition_name FROM conditions", conn).set_index('condition_name')['id']
                cond_df['condition_id'] = cond_df['condition_name'].map(cond_map)

                # Studies
                studies.to_sql('studies', conn, if_exists='append', index=False)

                # Relaciones
                relations = cond_df[['study_key', 'condition_id']].dropna()
                if not relations.empty:
                    relations.to_sql('study_conditions', conn, if_exists='append', index=False)

        logging.info("Carga completada con éxito ✓")
    except Exception as e:
        logging.error(f"Error en PostgreSQL: {e}")
        raise


if __name__ == "__main__":
    cargar_datos()