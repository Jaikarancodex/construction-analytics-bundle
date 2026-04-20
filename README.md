# Construction Analytics — Cost & Performance Control Tower

> End-to-end intelligence platform for Finnish construction, built on **Databricks Lakehouse**.

Turns raw construction project data into predictive insights that prevent cost overruns, reduce schedule delays, and protect project margins.

## Architecture
```
RAW DATA (Bronze) → CLEANSED (Silver) → BUSINESS KPIs (Gold) → ML MODEL → DECISIONS
  8 Raw Tables       8 Cleansed          5 Gold KPIs         Risk Scores   2 Dashboards
  ~14K records       Tables              + 2 ML Tables       & Predictions + Genie Space
```

## Folder Structure
```
construction-analytics-bundle/
├── databricks.yml                       # DAB bundle config (dev/staging/prod)
├── resources/
│   ├── construction_analytics_pipeline.yml
│   └── construction_analytics_job.yml
├── src/
│   ├── config/                          # Project configuration
│   ├── data_generation/                 # Synthetic data generator
│   ├── pipeline/                        # SDP SQL definitions
│   │   ├── bronze/  (8 streaming tables)
│   │   ├── silver/  (8 cleansed tables)
│   │   └── gold/    (5 materialized views)
│   ├── transformations/                 # Batch ETL notebooks
│   ├── ml/                              # Feature eng + LightGBM model
│   └── governance/                      # DQ audit + UC governance tags
├── dashboards/
├── tests/                               # Pipeline validation tests
├── docs/                                # Business walkthrough
└── utilities/                           # Dashboard setup, Genie, teardown
```

## Job Workflow DAG
```
data_generator → bronze_to_silver → silver_to_gold ─┬→ feature_engineering → ml_model ─┐
                                                     ├→ data_quality_audit ─────────────┤
                                                     └→ uc_governance_tags ─────────────┤
                                                                                        └→ pipeline_validation_tests
```
Schedule: Daily at 06:00 (Europe/Helsinki)

## Quick Start
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run construction_analytics_workflow -t dev
```

## Catalogs
| Target | Catalog |
|--------|---------|
| dev | `construction_demo` |
| staging | `construction_staging` |
| prod | `construction_prod` |

---
*Built with Databricks Asset Bundles • Unity Catalog • SDP Pipelines • MLflow • Lakeview Dashboards*
