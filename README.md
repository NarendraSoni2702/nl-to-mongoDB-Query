# Natural Language to MongoDB Aggregation Parser

## Overview

This project converts natural language queries into MongoDB aggregation pipelines.

### Features

- Parses conditions with `$match` (`$and`, `$or`)
- Supports `$project`, `$switch`, `$filter`, `$group`, `$unwind`
- Includes a test harness with rich, color-coded output
- Provides a simple Streamlit web UI for interactive use

---

## Setup

1. Clone or unzip the repo
2. Create a virtual environment (recommended)
3. Install dependencies:

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
