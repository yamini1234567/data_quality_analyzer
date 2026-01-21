# Data Quality Analyzer Framework - RCM

Feature readiness validation framework for Revenue Cycle Management (RCM) AI features.

## Overview

This framework validates whether your data is ready for AI-powered RCM features. Currently supports:

- **Additional Charge Feature**: Validates diagnosis-CPT pattern data readiness

## Features

- ✅ Automated readiness checks
- ✅ MongoDB integration
- ✅ Configurable thresholds
- ✅ Detailed validation reports
- ✅ Historical stats generation

## Requirements

- Python 3.10+
- MongoDB 4.0+
- Required packages (see ai_core/requirements.txt)

## Installation

1. Clone the repository

```bash
git clone <repository-url>
cd DataQualityAnalyzer_framework_RCM
```

2. Install dependencies

```bash
pip install -r ai_core/requirements.txt
```

3. Configure MongoDB connection

```bash
cp .env.example .env
# Edit .env with your MongoDB URI
```

## Usage

### Run Readiness Checks

```bash
python main.py
```

### Generate Historical Stats

```bash
# Generate payer-CPT stats
python scripts/generate_stats_collection.py

# Generate diagnosis-CPT pattern stats
python scripts/generate_diagnosis_stats.py
```

### Create App Settings

```bash
python scripts/create_app_settings.py
```

## Configuration

Edit `.env` file:

```env
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB_NAME=rcm_test_db
```

## Readiness Checks

### Additional Charge Feature

1. **App Settings Validation** - Verifies configuration exists
2. **Claims with Diagnoses** - Validates diagnosis data availability
3. **Diagnosis Code Diversity** - Checks for sufficient ICD-10 codes
4. **Diagnosis-CPT Pattern Stats** - Validates historical pattern data
5. **Data Quality** - Samples and validates stat quality

## Project Structure

```
├── main.py                          # Entry point
├── ai_core/
│   └── feature_readiness/
│       ├── base_standalone.py       # Base classes
│       ├── appsettings.py          # Configuration models
│       ├── models.py                # Data models
│       └── checks/
│           └── additional_charge_checks.py  # Readiness checks
├── scripts/
│   ├── generate_stats_collection.py         # Payer-CPT stats generator
│   ├── generate_diagnosis_stats.py          # Diagnosis-CPT stats generator
│   ├── create_app_settings.py              # App settings creator
│   └── load_data.py                        # Sample data loader
├── shared/
│   ├── db.py                       # Database utilities
│   └── utils.py                    # Helper functions
└── tests/
    └── MongoDB_Connection.py       # Connection tests
```

## Output Example

```
================================================================================
ADDITIONAL CHARGE READINESS VALIDATION
================================================================================

Total Checks:       5
Passed:            4
Failed:             1
Readiness Score:   80.0%

STATUS: MOSTLY READY - Some issues to address
================================================================================
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
