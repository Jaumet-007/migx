import pandas as pd
from sqlalchemy import create_engine, text

# 1. Configuración de la conexión (Puerto 5434 de tu nuevo Docker)
engine = create_engine("postgresql://migx_user:migx_password@localhost:5434/clinical_db")

def cargar_datos():
    print("📖 Leyendo clin_trials.csv...")
    # Usamos 'r' para evitar el error de ruta de nuevo
    df = pd.read_csv(r'G:\02-Elling\MIGx\dataset_csv\clin_trials.csv')

    # --- TRANSFORMACIÓN ---
    df['Start Date'] = pd.to_datetime(df['Start Date'], errors='coerce')

    # 1. Tabla de Estudios
    studies = df[['Organization Full Name', 'Overall Status', 'Start Date', 'Phases', 'Study Type']].copy()
    studies.columns = ['org_name', 'status', 'start_date', 'phase', 'study_type']
    studies.index.name = 'study_id'

    # 2. Tabla de Condiciones (CON ELIMINACIÓN DE DUPLICADOS)
    conditions_list = []
    for idx, row in df.iterrows():
        if pd.notna(row['Conditions']):
            # Usamos set() para eliminar duplicados dentro de la misma fila
            parts = set([c.strip() for c in str(row['Conditions']).split(',')])
            for p in parts:
                if p: # Evitar strings vacíos
                    conditions_list.append({'study_id': idx, 'condition_name': p})
    
    cond_df = pd.DataFrame(conditions_list)
    # Eliminamos duplicados generales por si acaso
    cond_df = cond_df.drop_duplicates()

    # --- CARGA A POSTGRES ---
    print("📥 Insertando datos en las tablas...")
    try:
        with engine.begin() as conn:
            # Vaciamos tablas antes de cargar para evitar errores de clave primaria si re-ejecutas
            conn.execute(text("TRUNCATE studies, study_conditions CASCADE"))
            
            studies.to_sql('studies', conn, if_exists='append', index=True)
            cond_df.to_sql('study_conditions', conn, if_exists='append', index=False)
        print("✅ ¡Datos cargados con éxito en clinical_db!")
    except Exception as e:
        print(f"❌ Error al cargar: {e}")

if __name__ == "__main__":
    cargar_datos()