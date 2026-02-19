# Clinical Trials Analytics Report

## 1. Volume and General Status

The consolidated dataset contains **495,634 studies**, managed by **28,081 organizations**. Most trials are in `COMPLETED` status (54.6%), followed by those in `RECRUITING` phase (14.3%). **108,142 unique clinical conditions** have been identified, reflecting the high diversity of the medical ecosystem analyzed.

---

## 2. Conditions and Medical Complexity

**Top Conditions.** Studies in "Healthy" subjects lead the ranking with over 10,000 trials. Followed by high-prevalence conditions such as breast cancer, obesity and diabetes mellitus.

**Condition Distribution.** The majority of studies (61%) focus on a single condition. However, there is a minority group of studies with complex clinical burden, reaching trials with more than 20 associated conditions.

---

## 3. Temporal Analysis and Performance

**Evolution.** Peak activity is observed between 2020 and 2022, likely driven by post-pandemic research.

**Completion Rate.** The oldest years show completion rates above 80%, while recent years (2023-2024) reflect low rates due to most studies still in progress.

**Longevity.** Historical records with durations exceeding 100 years have been detected (e.g. studies started in 1916/1917), requiring manual validation to rule out registration errors.

---

## 4. Industry Leaders (Top 10 Organizations)

The **National Cancer Institute (NCI)** leads the list with over 18,800 studies, followed by the **National Institutes of Health (NIH)**. Notably, pharmaceutical organizations like GlaxoSmithKline show very high completion efficiency (87.6%), compared to academic centers that typically handle longer-term studies.

---

## 5. Data Quality Audit (Critical Fields)

Data quality analysis reveals urgent areas for improvement for model reliability:

**Completeness.** Average completeness rate is 55.7%.

**Dates.** There are 219,166 records with missing dates (44% of total), limiting historical trend analysis.

**Anomalies.** 32 studies with future dates (after current date) were detected, confirming the need to implement validation rules in the ingestion layer.