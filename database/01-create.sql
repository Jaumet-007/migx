DROP TABLE IF EXISTS public.study_conditions;
DROP TABLE IF EXISTS public.studies;

CREATE TABLE public.studies (
    study_id INTEGER PRIMARY KEY,
    org_name TEXT,
    status TEXT,
    start_date DATE,
    phase TEXT,
    study_type TEXT
);

CREATE TABLE public.study_conditions (
    study_id INTEGER REFERENCES studies(study_id),
    condition_name TEXT,
    PRIMARY KEY (study_id, condition_name)
);

-- 3. Índices para optimizar las analíticas (Bonus de rendimiento)
CREATE INDEX idx_studies_status ON public.studies(status);
CREATE INDEX idx_studies_phase ON public.studies(phase);
CREATE INDEX idx_conditions_name ON public.study_conditions(condition_name);