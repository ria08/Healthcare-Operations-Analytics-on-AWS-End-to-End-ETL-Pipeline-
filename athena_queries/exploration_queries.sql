-- ============================================================
-- HEALTHCARE OPERATIONS ANALYTICS — ATHENA EXPLORATION QUERIES
-- Database: healthcare_curated_db
-- Purpose: Validate KPI outputs and surface business insights
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- QUERY 1: Excess Readmissions vs Cardiology Benchmark
-- Cardiology (7.9%) used as the efficiency benchmark.
-- Shows how many readmissions each department has ABOVE that rate.
-- i.e. patients who came back within 30 days and theoretically
-- shouldn't have, if the department matched Cardiology's performance.
-- ──────────────────────────────────────────────────────────────
SELECT
    medical_specialty,
    total_visits,
    readmit_30_day_rate_pct,
    readmit_30_day_count,
    ROUND((readmit_30_day_rate_pct - 7.9) / 100 * total_visits) AS excess_readmissions
FROM readmission_by_department
WHERE readmit_30_day_rate_pct > 7.9
AND total_visits >= 500
ORDER BY excess_readmissions DESC;

-- Result: InternalMedicine leads with 483 excess readmissions,
-- followed by Family/GeneralPractice (298) and Emergency/Trauma (250).
-- Total across all 12 underperforming departments: 1,429 excess readmissions.


-- ──────────────────────────────────────────────────────────────
-- QUERY 2: Department Efficiency Band Classification
-- Combines avg LOS + 30-day readmission rate into a 0-100 score.
-- Higher = more efficient. Classifies into High / Medium / Low bands.
-- ──────────────────────────────────────────────────────────────
SELECT
    medical_specialty,
    total_visits,
    avg_los_days,
    readmit_30_day_rate_pct,
    efficiency_score,
    CASE
        WHEN efficiency_score >= 80 THEN 'High Efficiency'
        WHEN efficiency_score >= 50 THEN 'Medium Efficiency'
        ELSE 'Low Efficiency'
    END AS efficiency_band
FROM readmission_by_department
WHERE total_visits >= 500
ORDER BY efficiency_score DESC;

-- Result: ObstetricsandGynecology scores 100.0 (best),
-- Cardiology 85.6, InternalMedicine 65.1, Nephrology 46.8 (worst).


-- ──────────────────────────────────────────────────────────────
-- QUERY 3: Readmission Rate by Age Group
-- Identifies which age bands have the highest 30-day readmission rates.
-- Counterintuitive finding: youngest adults (20-30) have highest rate.
-- ──────────────────────────────────────────────────────────────
SELECT
    age,
    total_patients,
    readmit_30_day_count,
    readmit_30_day_rate_pct
FROM readmission_by_age
ORDER BY readmit_30_day_rate_pct DESC;

-- Result: [20-30) = 14.2% — highest of all age groups.
-- [80-90) = 12.1%, [70-80) = 11.8%, [50-60) = 9.7% (lowest among adults).


-- ──────────────────────────────────────────────────────────────
-- QUERY 4: Insulin Risk by Age (Highest Risk Combinations)
-- Filters to patients with active insulin changes (Up or Down).
-- Shows which age + insulin change combinations carry the most risk.
-- ──────────────────────────────────────────────────────────────
SELECT
    age,
    insulin,
    total_patients,
    readmit_30_day_rate_pct
FROM readmission_risk_profile
WHERE insulin IN ('Up', 'Down')
AND total_patients >= 100
ORDER BY readmit_30_day_rate_pct DESC
LIMIT 10;

-- Result: [20-30) + Insulin Up = 21.0% readmission rate — highest risk combination.
-- [20-30) + Insulin Down = 19.5%. 1 in 5 young patients with insulin changes return within 30 days.


-- ──────────────────────────────────────────────────────────────
-- QUERY 5: Best vs Worst Departments — Head to Head
-- Direct comparison of top and bottom performing departments
-- across the key metrics: visits, LOS, readmission rate, efficiency.
-- ──────────────────────────────────────────────────────────────
SELECT
    medical_specialty,
    total_visits,
    avg_los_days,
    readmit_30_day_rate_pct,
    efficiency_score
FROM readmission_by_department
WHERE medical_specialty IN (
    'Cardiology',
    'ObstetricsandGynecology',
    'Nephrology',
    'InternalMedicine',
    'Family/GeneralPractice',
    'Emergency/Trauma'
)
ORDER BY efficiency_score DESC;

-- Result: Cardiology (5,352 visits, 3.53 days, 7.9%, score 85.6) vs
-- Nephrology (1,613 visits, 5.02 days, 15.4%, score 46.8).
-- InternalMedicine sees 3x more patients than Cardiology at similar
-- readmission rate but longer stay — medium efficiency at scale.


-- ──────────────────────────────────────────────────────────────
-- QUERY 6: Total Excess Readmissions — Headline Metric
-- Single number summarising the system-wide readmission problem
-- relative to the Cardiology benchmark.
-- ──────────────────────────────────────────────────────────────
SELECT
    SUM(ROUND((readmit_30_day_rate_pct - 7.9) / 100 * total_visits)) AS total_excess_readmissions,
    COUNT(medical_specialty) AS departments_above_benchmark
FROM readmission_by_department
WHERE readmit_30_day_rate_pct > 7.9
AND total_visits >= 500;

-- Result: 1,429 excess readmissions across 12 departments.
-- This is the project headline metric — the quantified scale of the problem.
