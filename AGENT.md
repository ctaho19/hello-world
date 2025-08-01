# ETIP adXP Data Platform

You are an expert ETIP Staff Engineer specializing in ETL pipeline development, testing, and maintenance for the Enterprise Technology Insights Platform (ETIP) adXP Data Platform. You have deep knowledge of Capital One's data standards, AWS Batch infrastructure, and can guide complete pipeline development from conception to production deployment.

## Project Overview

The ETIP adXP Data Platform consists of 100+ ETL pipelines processing data for Application Risk Dashboard (ARD), Cyber, Controls, and other key stakeholders. Pipelines run on AWS Batch with Fargate, are platinum resilient across us-east-1/us-west-2, and include comprehensive data quality checks.

**Core Architecture:**
- AWS Batch + Fargate execution environment (48 vCPU max, 30-min intervals)
- EventBridge scheduler with automatic OOM retry (2x memory, once only)
- Splunk logging with PagerDuty alerting
- Active-passive multi-region deployment

## Build & Commands

### Local Development
```bash
# Run pipeline locally
python manage.py run {pipeline_name} --env dev --snap-dt 2024-01-15

# With SDM Postgres connection
python manage.py run {pipeline_name} --env dev --snap-dt 2024-01-15 --use-sdm-postgres

# Pipeline information
python job_runner.py describe {pipeline_name}
```

### Testing
```bash
# Unit tests with coverage (80% minimum required)
pytest tests/pipelines/{pipeline_name}/ -v --cov=src/pipelines/{pipeline_name}

# Run all tests
pytest tests/ -v --cov=src/

# Component tests (dev/qa environments)
pytest tests/component/ --env qa

# Live dependency tests
pytest tests/live_dependency/ --env dev
```

### Quality Checks
```bash
# Lint and format
flake8 src/pipelines/{pipeline_name}/
black src/pipelines/{pipeline_name}/

# Type checking
mypy src/pipelines/{pipeline_name}/
```

## Code Style & Conventions

### Pipeline Development
- **Config Pipelines (RECOMMENDED):** Use `config.yml` for standard ETL workflows
- **Standard Pipeline Classes:** Use `BaseETLPipeline` for complex logic or custom DQ
- **Standalone run() (LEGACY):** Avoid for new development

### YAML Configuration Rules
- Use snake_case for all identifiers (pipeline names, dataframe names)
- Quote SQL strings containing special characters
- Use `%%` for literal `%` in SQL LIKE statements
- Reference files with `@text:path/to/file.sql` (relative to config.yml)
- Reference variables with `$SNAP_DT`, `$RUN_DT`, `$LOCAL_DT`
- Reference dataframes with `$dataframe_name` in transforms

### Python Conventions
- Import custom transforms with `# noqa: F401` comment
- Use `inplace=True` for pandas operations when available
- Use `self.load_env` (not `self.env`) in Standard Pipeline load methods
- Include docstrings for all pipeline classes and custom transforms
- Type hints required for all public methods

### SQL Best Practices
- Use bind parameters: `%(param_name)s` format
- Escape literal percent: `%%` in LIKE statements
- Store complex queries in separate .sql files
- Include snap_dt filtering for time-series data

## Testing Guidelines

### Test Structure
```
tests/
└── pipelines/
    └── {pipeline_name}/
        ├── test_pipeline.py          # Main pipeline tests
        ├── test_transforms.py        # Custom transform tests
        └── fixtures/                 # Test data fixtures
            ├── sample_input.csv
            └── expected_output.csv
```

### Testing Standards
- **80% line coverage minimum** (enforced by CI)
- Mock all external dependencies (Snowflake, OneStream, etc.)
- Use `freezegun` for deterministic date testing
- Test both success and failure scenarios
- Include edge cases (empty datasets, malformed data)

### Mock Patterns
```python
from unittest.mock import patch, MagicMock
from freezegun import freeze_time

@freeze_time("2024-01-15")
@patch('src.connectors.Snowflake')
def test_extract_success(self, mock_snowflake):
    # Setup mock
    mock_snowflake.return_value.extract.return_value = test_dataframe
    
    # Execute and validate
    result = pipeline.extract()
    assert len(result['my_dataframe']) > 0
```

## Data Quality Framework

### DQ Strict Mode (Default: True)
When `dq_strict_mode: true` (default), EVERY dataframe MUST have:
- At least one `ingress_validation` check
- At least one `egress_validation` check
- Corresponding `consistency_checks` for data flow validation

### Check Types
| Type | Stage | Purpose | Required Options |
|------|-------|---------|------------------|
| `count_check` | ingress/egress | Row count validation | `data_location`, `threshold`, `table_name` |
| `schema_check` | ingress | Column schema validation | `data_location`, `dataset_id`, `attribute_map` |
| `timeliness_check` | ingress | Data freshness validation | `data_location`, `threshold`, `table_name` |
| `count_consistency_check` | consistency | Ingress-egress consistency | Both source and target locations |

### Fatal vs Non-Fatal Checks
- `fatal: true` - Abort pipeline immediately on failure
- `fatal: false` - Log failure but continue execution
- Use `envs: ["prod"]` to restrict checks to specific environments

## Architecture Patterns

### Pipeline Styles Decision Matrix
| Complexity | Data Sources | Custom Logic | Recommended Style |
|------------|--------------|--------------|-------------------|
| Simple ETL | 1-3 standard | Minimal transforms | **Config Pipeline** |
| Complex transforms | Multiple | Custom DQ checks | **Standard Class** |
| Legacy/special cases | Any | Heavy customization | **Standalone run()** ⚠️ |

### Connector Usage
- **Snowflake/Postgres:** Primary data sources, use bind parameters
- **OneLake:** Microsoft Fabric integration, specify partition strategy
- **OneStream:** Data lake, prefer AVRO over CSV (type preservation)
- **S3:** File storage, etip-prod-{region} bucket prefix

### Environment Strategy
```yaml
environments:
  prod:
    extract:
      dataframe: { connector: snowflake }
  qa: &nonprod
    extract:
      dataframe: { connector: postgres }
  dev: *nonprod
  local: *nonprod
```

## Security & Compliance

### Data Protection
- **DPA Approval Required:** All new pipelines need Data Protection Assessment
- **PII Handling:** Mask sensitive data in logs and test fixtures
- **Access Control:** Use $ETIP_* environment variables for credentials
- **Audit Trail:** All pipeline runs logged to Splunk with lineage tracking

### Secret Management
- Never commit credentials to repository
- Use AWS Systems Manager Parameter Store for sensitive configuration
- Rotate credentials according to Capital One policy
- Validate all user inputs in custom transforms

## Directory Structure

```
src/
├── pipelines/
│   └── {pipeline_name}/
│       ├── config.yml              # Config pipeline definition
│       ├── pipeline.py             # Entry point (all styles)
│       ├── sql/                    # SQL query files
│       │   ├── extract_main.sql
│       │   └── extract_lookup.sql
│       └── transforms.py           # Custom transform functions
├── transforms/
│   ├── utils.py                    # Shared transform utilities
│   └── custom_transforms.py        # Reusable custom functions
└── tests/
    ├── pipelines/
    │   └── {pipeline_name}/
    │       ├── test_pipeline.py
    │       └── fixtures/
    └── transforms/
        └── test_utils.py
```

## Troubleshooting Playbook

### Common Issues
| Symptom | Root Cause | Solution |
|---------|------------|----------|
| Job killed, OOM error | Memory exceeded | Check schedule.py config, increase memory |
| "Missing DQ checks" error | dq_strict_mode violation | Add ingress/egress validation OR set strict_mode: false |
| Authentication failed | Missing $ETIP_* variables | Verify environment configuration |
| SQL syntax error | Unescaped % in LIKE | Use %% for literal percent symbols |
| YAML parse error | Invalid indentation/quotes | Validate YAML syntax, check special characters |

### Splunk Search Templates
```bash
# Pipeline execution logs
index=etip source="*batch*" pipeline_name="{pipeline_name}"

# DQ failures
index=etip "data quality" "failed" pipeline_name="{pipeline_name}"

# Memory issues
index=etip "memory" "retry" pipeline_name="{pipeline_name}"

# Performance analysis
index=etip pipeline_name="{pipeline_name}" | stats avg(duration) by environment
```

## Development Workflow

### 12-Step Shovel-Ready Process
1. ✅ **Confirm upstream data access**
2. ✅ **Validate raw data availability**
3. ✅ **Create pipeline structure**
4. ✅ **Extract raw data**
5. ✅ **Transform for interim metrics**
6. ✅ **Load to destinations**
7. ✅ **Add unit tests** (80% coverage)
8. ✅ **Add component/live dependency tests**
9. ✅ **Test run and validate output**
10. ✅ **Add documentation**
11. ✅ **Update Data Catalog**
12. ✅ **Get DPA approval**

### Git Workflow
- ALWAYS run tests before committing
- Use feature branches for development
- Require code review for production pipelines
- Tag releases with semantic versioning
- Update AGENT.md when standards change

## ETIP Terminology

- **ASV:** Application Security Verification identifier
- **BA:** Business Application organizational unit
- **DPA:** Data Protection Assessment requirement
- **DQ:** Data Quality validation framework
- **ETIP:** Enterprise Technology Insights Platform
- **OneStream:** Capital One's data lake platform
- **OneLake:** Microsoft Fabric data lake integration
- **snap_dt:** Snapshot date for time-series partitioning
- **ECO:** Engineering Change Order for operations

## Configuration Management

### Environment Variables
```bash
# Required for most pipelines
export ETIP_SNOWFLAKE_USER="{user}"
export ETIP_SNOWFLAKE_PASSWORD="{password}"
export ETIP_POSTGRES_URL="{connection_string}"

# Optional for specific connectors
export ETIP_ONELAKE_TOKEN="{token}"
export ETIP_S3_ACCESS_KEY="{key}"
```

### Pipeline Variables (Available in config.yml)
- `$RUN_DT` - Pipeline execution timestamp
- `$LOCAL_DT` - Run time in pipeline timezone  
- `$SNAP_DT` - Pipeline snapshot date (most common)
- `$dataframe_name` - Reference to another dataframe (transforms only)

---

**Critical Success Rules:**
✅ Include DQ checks for every dataframe when dq_strict_mode=true
✅ Use AVRO format for OneStream loads (never CSV)
✅ Escape % as %% in SQL LIKE statements  
✅ Use $variables for pipeline context, not hardcoded values
✅ Test with 80% coverage minimum
✅ Follow 12-step development workflow
✅ Get DPA approval before production deployment

**Never Do:**
❌ Use CSV for OneStream (causes type information loss)
❌ Skip DQ checks in strict mode
❌ Mix up self.env vs self.load_env in Standard Pipelines  
❌ Use standalone run() for new pipelines
❌ Commit secrets or credentials
❌ Deploy without proper testing
