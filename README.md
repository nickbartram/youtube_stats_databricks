# README for Youtube Dataset ETL Project
## Project Overview

This project implements an end-to-end ETL pipeline in Databricks using PySpark and a multi-region YouTube Trending Videos [dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new) sourced from Kaggle.

The pipeline ingests raw CSV and JSON files from multiple geographic regions, validates and transforms the data through Bronze, Silver, and Gold layers, and produces an analytics-ready dataset enriched with business-focused metrics such as engagement rate, comment rate, like ratio, and log-transformed views.

To improve maintainability and reusability, the project separates exploratory development, reusable ETL functions, pipeline orchestration, and downstream analytics into dedicated notebooks. The final Gold dataset is designed to support reporting, visualization, and further analytical workflows.

### Key Objectives:
- Build a reusable PySpark ETL pipeline using Databricks
- Implement Bronze, Silver, and Gold data layers
- Create automated validation and transformation functions
- Engineer business-focused engagement metrics
- Produce an analytics-ready dataset for downstream visualization

## Dataset

This project uses the YouTube Trending Videos Dataset, a collection of daily records of trending YouTube videos gathered through the YouTube API published on Kaggle.

The dataset contains trending videos information from multiple geographic regions, including Canada (CA), United States (US), Great Britain (GB), Germany (DE), France (FR), India (IN), Japan (JP), South Korea (KR), Mexico (MX), and Russsia (RU). Each region is provided as a separate dataset, making it well-suited for demonstrating multi-source ingestion and consolidation workflows.

### Data Summary

#### Raw Dataset (Ingestion Layer)

| Metric | Value |
|--------|-------|
| Total Size | 539.22 MB |
| Format | Multi-region CSV + JSON |
| Regions | 10 |

#### Gold Layer Dataset (Analytics-Ready)

| Metric | Value |
|--------|-------|
| Records | 207,142 |
| Columns | 19 |
| Estimated Size | 116.83 MB |

### Data Files

The dataset consists of two primary file types:

#### Video Data (CSV)

Regional CSV files containing video-level metrics and metadata, incudling:

- Video ID
- Video title
- Channel title
- Publish timestamp
- Trending date
- View count
- Like count
- Dislike count
- Comment count
- Status flags and additional metadata

#### Examples:

- CAvideos.csv
- USvideos.csv
- GBvideos.csv

### Category Metadata (JSON)

Regional JSON files containing mappings between YouTube category IDs and category names.

#### Examples:

- CA_category_id.json
- US_category_id.json
- GB_category_id.json

These files were used to enrich the video data through dimension-style joins, replacing category IDs with human-readable category names.

### ETL Considerations

This dataset presented several challenges commonly encountered in real-world data engineering workflows:

- Multi-line CSV records requiring specialized ingestion settings.
- Nested JSON structures requiring flattening and transformation.
- Multiple source files requiring schema validation before union operations.
- Mixed data types requiring explicit casting and cleaning.
- Regional category metadata requiring enrichment joins.
- Source lineage preservation through region tracking.

The final ETL pipeline consolidates all regional datasets into a unified analytics-ready Gold dataset suitable for reporting, visualization, and further analysis.

## Architecture

This projecct follows a simplified Medallion-style ETL architecture implemented in PySpark within Databricks. The pipeline transforms raw multi-region YouTube trending data into an analytics-ready dataset.

```mermaid
graph TD
    A[Raw CSVs + JSON (Multi-Region)] --> B[Bronze Layer: Ingestion]
    B --> C[Silver Layer: Cleaning & Type Casting]
    C --> D[Gold Layer: Joins & Enrichment]
    D --> E[Feature Engineering]
    E --> F[Final Dataset: youtube_gold Spark Table]
```
Bronze Layer (Ingestion)
- Loads raw CSV and JSON files using Spark
- Adds region metadata for lineage tracking
- Performs basic ingestion validation

Silver Layer (Transformation)
- Standardizes schema across regions
- Casts timestamps and numeric fields
- Prepares clean dataset for joins

Gold Layer (Enrichment)
- Joins video data with category metadata
- Flattens nested JSON structures
- Produces unified analytical dataset

Feature Engineering
- Computes engagement metrics (like ratio, engagement rate, comment rate)
- Applies log transformation to views
- Creates derived analytical features for insights

Final Output
- Stored as Spark table: `youtube_gold`
- Ready for analysis and visualization

## ETL Pipeline

This project implements a modular ETL pipeline in PySpark, structured as a series of reusable functions that progressively transform raw multi-region YouTube trending data into an analytics-ready dataset.

---

### Bronze Layer (Raw Data Ingestion & Validation)

Responsible for loading raw CSV and JSON files and performing initial validation.

**Ingestion Functions:**
- `load_raw_csv()`
- `load_all_regions()`
- `load_raw_json()`
- `load_all_category_jsons()`

**Validation Functions:**
- `validate_bronze`
- `validate_category_raw`

**Responsibilities:**
- Load raw multi-region datasets
- Apply file-level ingestion logic
- Add region lineage metadata
- Validate schema consistency at ingestion stage

---

### Silver Layer (Cleaning & Standardization)

Responsible for transforming raw data into a clean, structured format.

**Functions:**
- `transform_silver()`
- `deduplicate_trending()`
- `validate_silver()`

**Responsibilities:**
- Cast and standardize data types
- Remove duplicate records
- Ensure schema consistency across regions
- Prepare dataset for enrichment and joining

---

### Gold Layer (Enrichment & Feature Preparation)

Responsible for building analytical structures and preparing final dataset schema.

**Functions:**
- `flatten_categories()`
- `build_category_dim()`
- `finalize_gold_schema()`

**Responsibilities:**
- Flatten nested category JSON structures
- Build category dimension table
- Define final analytical schema
- Prepare join-ready dataset structure

---

### Feature Engineering Layer

Responsible for deriving business metrics used for analysis.

**Functions:**
- `safe_div()`
- `add_business_metrics()`

**Features Created:**
- `like_ratio`
- `engagement_rate`
- `comment_rate`
- `log_views`

**Responsibilities:**
- Compute engagement-based metrics
- Handle division safety for ratio features
- Apply log transformation for skewed distributions

---

### Output Layer

Final persistence layer for analytics and visualization.

**Functions:**
- `write_gold_table`

**Output:**
- `youtube_gold` (Spark table)

**Responsibilities:**
- Persist final curated dataset
- Enable downstream analysis and visualization workflows

## Data Model

The final dataset is structured as a flat analytical table (`youtube_gold`) designed for exploratory analysis and business intelligence use cases.

Unlike a fully normalized warehouse model, this project uses a **denormalized analytical schema** to optimize for readability and downstream analysis in Spark and visualization tools.

---

### Table: youtube_gold

The dataset contains video-level observations enriched with category metadata and engineered business metrics.

#### Core Identifiers
- `video_id`
- `category_id`
- `category_name`
- `region`

#### Video Metadata
- `title`
- `channel_title`
- `publish_time`
- `trending_date`
- `tags`
- `thumbnail_link`
- `description`

#### Engagement Metrics
- `views`
- `likes`
- `dislikes`
- `comment_count`

#### Flags
- `comments_disabled`
- `ratings_disabled`
- `video_error_or_removed`

#### Engineered Features
- `like_ratio`
- `engagement_rate`
- `comment_rate`
- `log_views`

---

### Design Choice

This dataset follows a **flattened analytical model** rather than a multi-table star schema because:

- The primary use case is exploratory analysis
- Spark-based workflows benefit from denormalized structures
- Category information is lightweight enough to embed directly
- Feature engineering is performed at the row level

This design prioritizes **simplicity, performance, and usability over normalization**.

## Feature Engineering

## Feature Engineering

The dataset includes several derived metrics designed to measure video engagement and audience interaction.

These features enable comparison across videos and regions independent of raw view counts.

Key engineered features include:
- engagement rate (overall interaction intensity)
- like ratio (positive reception proxy)
- comment rate (audience participation)
- log-transformed views (distribution normalization)

## Analysis & Visualizations

This section explores patterns in YouTube trending video data using the engineered features and aggregated dataset. The goal is to understand engagement behavior across videos, categories, and regions.

---

### Key Insights

- Engagement rates are generally low, with most videos concentrated near 0-10% interaction relative to views.
- A small number of videos exhibit significantly higher engagement, indicating a highly skewed distribution.
- Comment activity is much lower than like activity across all regions, suggesting passive engagement is dominant.
- View counts follow a heavy-tailed distribution, which is stabilized using log transformation (`log_views`).

---

### Engagement Rate Distribution

The distribution of engagement rate shows a strong right skew, with most videos clustered near zero and a long tail of highly engaging outliers.

*(Insert histogram of `engagement_rate` here)*

---

### View Distribution (Log Scale)

Raw view counts are heavily skewed, so a log transformation is applied to normalize the distribution for analysis.

*(Insert histogram of `log_views` here)*

---

### Like Ratio vs Views

Higher-view videos do not necessarily have higher like ratios, suggesting that virality and positive engagement are not strongly correlated.

*(Insert scatter plot: views vs like_ratio)*

---

### Commentary on Findings

Overall, the dataset suggests that YouTube engagement is highly skewed and driven by a small subset of viral videos, while the majority of content receives relatively low interaction levels.

## Technology Used

### Data Processing
- Databricks
- Apache Spark (PySpark)

### Programming
- Python

### Data Analysis & Visualization
- Pandas
- Plotly

### Data Formats
- CSV
- JSON

### Version Control
- GitHub

## Future Improvements

While this project implements a complete end-to-end ETL pipeline, several enhancements could further improve scalability, maintainability, and production readiness.

---

### Orchestration

- Introduce workflow orchestration using tools such as **Apache Airflow** or **Databricks Workflows**
- Automate pipeline execution with scheduled or event-driven triggers

---

### Incremental Processing

- Transition from full dataset reprocessing to **incremental ingestion**
- Use partitioning (e.g., by `region` or `trending_date`) to improve performance
- Reduce recomputation costs for large-scale datasets

---

### Data Lakehouse Enhancements

- Store data in **Delta Lake format** to enable ACID transactions
- Implement schema enforcement and evolution for safer updates
- Improve reliability of Bronze/Silver/Gold layers

---

### Data Quality Framework

- Expand validation layer with automated data quality checks
- Add constraints for null handling, ranges, and referential integrity
- Track data quality metrics over time

---

### Scalability Improvements

- Optimize Spark jobs using partitioning and caching strategies
- Evaluate performance improvements for large-scale multi-region joins

---

### Advanced Analytics

- Extend feature engineering with time-based trends (weekly/monthly aggregation)
- Add segmentation by category or region for deeper insights
- Incorporate predictive modeling (e.g., engagement prediction)

## Usage

This project is designed to be executed in Databricks using a notebook-based workflow. The pipeline is modular and runs in a defined sequence from raw ingestion to final table creation.

---

### 1. Load ETL Functions

First, load the reusable ETL functions notebook:

```python
%run "/Users/<your-path>/youtube_etl_functions"
```

### Run Exploration / Setup (Optional)

The `exploration_notebook` was used for `initial data discovery and function development. This step is not required for pipeline execution.

### Execute ETL Pipeline Notebook
Run the main pipeline notebook:
- `youtube_etl_pipeline`
This will:
- Load raw CSV and JSON data
- Perform validation checks
- Transform data through Bronze → Silver → Gold layers
- Generate feature-engineered dataset

### Create Final Gold Table
The final output is written to a Spark table:
```python
youtube_gold
```
This table contains the fully processed dataset ready for analysis.

### Load Data for Analysis (Optional)
For downstream analysis or visualizations:
```python
df_gold = spark.table("youtube_gold")
```

## References

### Dataset
- YouTube Trending Video Dataset (Kaggle)  
  https://www.kaggle.com/datasets/datasnaek/youtube-new

---

### Documentation
- Apache Spark Documentation  
  https://spark.apache.org/docs/latest/

- Databricks Documentation  
  https://docs.databricks.com/

---

### Tools Used
- ChatGPT (OpenAI) - Used for assistance with code structuring, documentation refinement, and debugging support during development
- Claude (Anthropic) - Used for supplementary explanation and architectural discussion during pipeline design

