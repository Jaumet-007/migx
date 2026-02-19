# data_quality_report.py
# Generates a data quality validation report for the 3 tables
# User-friendly for non-technical users
# Run after loading to detect issues

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Configuration (adjust according to your environment)
DB_URL = "postgresql://migx_user:migx_password@localhost:5434/clinical_db"
OUTPUT_FILE = "informe_limpieza_datos.txt"

engine = create_engine(DB_URL)

def generate_report():
    """Generates user-friendly data quality report"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    lines = [
        "╔" + "═"*78 + "╗",
        "║" + " DATA QUALITY REPORT - CLINICAL TRIALS ".center(78) + "║",
        "║" + f" Generated: {timestamp}".ljust(78) + "║",
        "╚" + "═"*78 + "╝",
        ""
    ]

    general_status = True  # Flag for overall status

    # 1. UNIQUENESS IN CONDITIONS
    lines.append("┌─ VALIDATION 1: Unique Condition Names")
    lines.append("└─ Purpose: Avoid duplicates of common conditions")
    try:
        df = pd.read_sql("""
            SELECT condition_name, COUNT(*) AS count
            FROM conditions
            GROUP BY condition_name
            HAVING COUNT(*) > 1
            ORDER BY count DESC;
        """, engine)
        
        if len(df) == 0:
            lines.append("   ✓ STATUS: OK - No duplicate conditions")
            lines.append("   Metric: 0 duplicates")
        else:
            general_status = False
            lines.append("   ✗ STATUS: PROBLEMS DETECTED")
            lines.append(f"   Metric: {len(df)} duplicate conditions")
            lines.append("   Action: Investigate CSV source to fix variations (e.g. 'diabetes' vs 'Diabetes')")
            for _, row in df.iterrows():
                lines.append(f"     • '{row['condition_name']}': {row['count']} records")
    except Exception as e:
        lines.append(f"   ✗ ERROR: {str(e)}")
        general_status = False
    
    lines.append("")

    # 2. REFERENTIAL INTEGRITY
    lines.append("┌─ VALIDATION 2: Reference Integrity (conditions and studies)")
    lines.append("└─ Purpose: Ensure all relationships are valid")
    
    try:
        # Orphan conditions
        df_cond = pd.read_sql("""
            SELECT COUNT(DISTINCT sc.condition_id) as orphans
            FROM study_conditions sc
            LEFT JOIN conditions c ON sc.condition_id = c.id
            WHERE c.id IS NULL;
        """, engine)
        orphans_cond = df_cond['orphans'].iloc[0]
        
        # Orphan studies
        df_stud = pd.read_sql("""
            SELECT COUNT(DISTINCT sc.study_key) as orphans
            FROM study_conditions sc
            LEFT JOIN studies s ON sc.study_key = s.study_key
            WHERE s.study_key IS NULL;
        """, engine)
        orphans_stud = df_stud['orphans'].iloc[0]
        
        total_issues = orphans_cond + orphans_stud
        
        if total_issues == 0:
            lines.append("   ✓ STATUS: OK - All references are valid")
            lines.append("   Metric: 0 broken references")
        else:
            general_status = False
            lines.append("   ✗ STATUS: PROBLEMS DETECTED")
            lines.append(f"   Metric: {total_issues} invalid references")
            if orphans_cond > 0:
                lines.append(f"     • {orphans_cond} condition ID(s) that don't exist in conditions table")
            if orphans_stud > 0:
                lines.append(f"     • {orphans_stud} study key(s) that don't exist in studies table")
            lines.append("   Action: Review CSV source for sequential load inconsistencies")
    except Exception as e:
        lines.append(f"   ✗ ERROR: {str(e)}")
        general_status = False
    
    lines.append("")

    # 3. COMPLETENESS IN KEY FIELDS
    lines.append("┌─ VALIDATION 3: Required Fields Complete")
    lines.append("└─ Purpose: Ensure essential data is not missing")
    
    try:
        df = pd.read_sql("""
            SELECT 
                COUNT(*) AS total,
                COUNT(CASE WHEN brief_title IS NULL OR brief_title = '' THEN 1 END) AS empty_titles,
                COUNT(CASE WHEN org_name IS NULL OR org_name = '' THEN 1 END) AS empty_orgs,
                COUNT(CASE WHEN overall_status IS NULL THEN 1 END) AS empty_statuses,
                COUNT(CASE WHEN start_date IS NULL THEN 1 END) AS empty_dates
            FROM studies;
        """, engine)
        
        total = df['total'].iloc[0]
        titles = df['empty_titles'].iloc[0]
        orgs = df['empty_orgs'].iloc[0]
        statuses = df['empty_statuses'].iloc[0]
        dates = df['empty_dates'].iloc[0]
        
        total_empty = titles + orgs + statuses + dates
        
        if total_empty == 0:
            lines.append("   ✓ STATUS: OK - All required fields are complete")
            lines.append(f"   Metric: {total} studies, 0 empty fields")
        else:
            general_status = False
            lines.append("   ✗ STATUS: EMPTY FIELDS DETECTED")
            lines.append(f"   Metric: {total_empty} empty fields out of {total * 4} total")
            lines.append(f"     • Empty titles: {titles}/{total}")
            lines.append(f"     • Empty organizations: {orgs}/{total}")
            lines.append(f"     • Empty statuses: {statuses}/{total}")
            lines.append(f"     • Empty start dates: {dates}/{total}")
            lines.append("   Action: Investigate CSV source for imputation or record filtering")
    except Exception as e:
        lines.append(f"   ✗ ERROR: {str(e)}")
        general_status = False
    
    lines.append("")

    # 4. DATE CONSISTENCY
    lines.append("┌─ VALIDATION 4: Logical Start Dates")
    lines.append("└─ Purpose: Detect impossible or inconsistent dates")
    
    try:
        df = pd.read_sql("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN start_date IS NULL THEN 1 END) as nulls,
                COUNT(CASE WHEN start_date > CURRENT_DATE THEN 1 END) as future,
                COUNT(CASE WHEN start_date >= '2026-01-01' THEN 1 END) as year_2026_plus
            FROM studies;
        """, engine)
        
        total = df['total'].iloc[0]
        nulls = df['nulls'].iloc[0]
        future = df['future'].iloc[0]
        year_plus = df['year_2026_plus'].iloc[0]
        
        date_issues = nulls + future
        
        if date_issues == 0:
            lines.append("   ✓ STATUS: OK - All dates are valid")
            lines.append(f"   Metric: {total} studies, 0 problematic dates")
        else:
            general_status = False
            lines.append("   ✗ STATUS: PROBLEMATIC DATES")
            lines.append(f"   Metric: {date_issues}/{total} studies with suspicious dates")
            if nulls > 0:
                lines.append(f"     • Missing dates (NULL): {nulls}")
            if future > 0:
                lines.append(f"     • Dates in the future (impossible): {future}")
            lines.append("   Action: Review records with problematic dates in CSV source")
    except Exception as e:
        lines.append(f"   ✗ ERROR: {str(e)}")
        general_status = False
    
    lines.append("")

    # 5. OUTLIERS IN NUMBER OF CONDITIONS
    lines.append("┌─ VALIDATION 5: Number of Conditions per Study")
    lines.append("└─ Purpose: Detect studies with no conditions or too many")
    
    try:
        df = pd.read_sql("""
            SELECT 
                COUNT(CASE WHEN num_cond = 0 THEN 1 END) as no_conditions,
                COUNT(CASE WHEN num_cond > 10 THEN 1 END) as many_conditions,
                COUNT(*) as total_studies
            FROM (
                SELECT COUNT(condition_id) as num_cond
                FROM study_conditions
                GROUP BY study_key
            ) subq;
        """, engine)
        
        no_cond = df['no_conditions'].iloc[0]
        many = df['many_conditions'].iloc[0]
        total_est = df['total_studies'].iloc[0]
        
        issues = no_cond + many
        
        if issues == 0:
            lines.append("   ✓ STATUS: OK - Normal distribution of conditions per study")
            lines.append(f"   Metric: {total_est} studies, all with 1-10 conditions")
        else:
            general_status = False
            lines.append("   ✗ STATUS: OUTLIERS DETECTED")
            lines.append(f"   Metric: {issues}/{total_est} studies with anomalous distribution")
            if no_cond > 0:
                lines.append(f"     • Studies with no conditions: {no_cond}")
            if many > 0:
                lines.append(f"     • Studies with >10 conditions: {many}")
            lines.append("   Action: Investigate for errors in multi-value split (CSV)")
    except Exception as e:
        lines.append(f"   ✗ ERROR: {str(e)}")
        general_status = False
    
    lines.append("")

    # 6. DUPLICATES
    lines.append("┌─ VALIDATION 6: Duplicates by Title + Organization")
    lines.append("└─ Purpose: Identify repeated or partially duplicate studies")
    
    try:
        df = pd.read_sql("""
            SELECT COUNT(*) as num_duplicate_groups
            FROM (
                SELECT brief_title, org_name, COUNT(*) as count
                FROM studies
                GROUP BY brief_title, org_name
                HAVING COUNT(*) > 1
            ) subq;
        """, engine)
        
        num_groups = df['num_duplicate_groups'].iloc[0]
        
        if num_groups == 0:
            lines.append("   ✓ STATUS: OK - No duplicates detected")
            lines.append("   Metric: 0 duplicate groups")
        else:
            general_status = False
            lines.append("   ✗ STATUS: PARTIAL DUPLICATES DETECTED")
            lines.append(f"   Metric: {num_groups} groups of duplicate studies")
            
            # Show the main ones
            df_details = pd.read_sql("""
                SELECT 
                    brief_title,
                    org_name,
                    COUNT(*) AS num_records,
                    COUNT(DISTINCT start_date) as distinct_dates
                FROM studies
                GROUP BY brief_title, org_name
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 5;
            """, engine)
            
            for _, row in df_details.iterrows():
                lines.append(f"     • '{row['brief_title'][:50]}...' / '{row['org_name'][:30]}...'")
                lines.append(f"       → {row['num_records']} records ({row['distinct_dates']} dates)")
            
            lines.append("   Action: Keep most recent/oldest records per team criteria")
    except Exception as e:
        lines.append(f"   ✗ ERROR: {str(e)}")
        general_status = False
    
    lines.append("")
    lines.append("")

    # FINAL SUMMARY
    lines.append("╔" + "═"*78 + "╗")
    if general_status:
        lines.append("║" + " ✓ OVERALL STATUS: GOOD DATA QUALITY ".center(78) + "║")
        lines.append("║" + " Data is ready for analytics ".center(78) + "║")
    else:
        lines.append("║" + " ✗ OVERALL STATUS: REVIEW DETECTED PROBLEMS ".center(78) + "║")
        lines.append("║" + " Investigate CSV source for corrections ".center(78) + "║")
    lines.append("╚" + "═"*78 + "╝")

    # Save report
    content = "\n".join(lines)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(content)

    print(content)
    print(f"\n✓ Report saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_report()