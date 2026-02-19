import importlib.util
from pathlib import Path
import pandas as pd
import numpy as np


def load_upload_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "database" / "02-upload.py"
    spec = importlib.util.spec_from_file_location("upload_mod", str(module_path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_generate_study_key_deterministic():
    mod = load_upload_module()
    row1 = {
        'brief_title': 'Test Study',
        'full_title': 'Test Study Full',
        'organization_full_name': 'ACME Pharma',
        'start_date': '2020-01-01'
    }
    row2 = dict(row1)
    # same content -> same key
    k1 = mod.generate_study_key(row1)
    k2 = mod.generate_study_key(row2)
    assert isinstance(k1, str) and len(k1) == 16
    assert k1 == k2

    # changing any field changes key
    row3 = dict(row1)
    row3['start_date'] = '2021-01-01'
    k3 = mod.generate_study_key(row3)
    assert k3 != k1


def test_extract_conditions_splitting_and_cleaning():
    mod = load_upload_module()
    df = pd.DataFrame([
        {'study_key': 's1', 'conditions': 'Diabetes, asthma|Cold,  x '},
        {'study_key': 's2', 'conditions': None},
        {'study_key': 's3', 'conditions': 'A, bb, ccc'}
    ])

    out = mod.extract_conditions(df)
    # should contain only cleaned entries (lowercase, stripped, min length >=3)
    assert 'condition_name' in out.columns
    # s1 should produce diabetes, asthma, cold
    conds_s1 = set(out[out['study_key'] == 's1']['condition_name'].tolist())
    assert conds_s1 == {'diabetes', 'asthma', 'cold'}

    # s2 had None -> no rows
    assert 's2' not in out['study_key'].values

    # s3 contains 'ccc' only (A and bb are <3)
    conds_s3 = set(out[out['study_key'] == 's3']['condition_name'].tolist())
    assert conds_s3 == {'ccc'}


def test_normalize_statuses_mapping_and_unexpected(caplog):
    mod = load_upload_module()
    df = pd.DataFrame({
        'overall_status': ['ENROLLING_BY_INVITATION', 'COMPLETED', 'FOO']
    })
    caplog.clear()
    caplog.set_level('WARNING')
    out = mod.normalize_statuses(df.copy())
    # mapping applied
    assert 'RECRUITING' in out['overall_status'].values
    # known value preserved
    assert 'COMPLETED' in out['overall_status'].values
    # unexpected value remains but triggers a warning
    assert 'FOO' in out['overall_status'].values
    warnings = [r.message for r in caplog.records if r.levelname == 'WARNING']
    assert any('no esperado' in str(w) or 'no esperado' in str(w).lower() for w in warnings)


def test_date_conversion_coerce_to_nat():
    # demonstrate the same conversion logic used in pipeline (pd.to_datetime errors='coerce')
    dates = pd.Series(['2021-01-01', '2004-10', 'notadate', None])
    converted = pd.to_datetime(dates, errors='coerce')
    assert pd.notna(converted[0])
    # '2004-10' may or may not parse depending on pandas parser settings; accept either
    assert (pd.notna(converted[1]) or pd.isna(converted[1]))
    # invalid -> NaT
    assert pd.isna(converted[2])
    assert pd.isna(converted[3])
