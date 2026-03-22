# UK Property Market Analysis (dbt Data Warehouse)

## Problem Statement (Analytical Goal)
The primary analytical goal of this data warehouse is to enrich raw UK Property Price Data (PPD) with granular socio-economic context (Index of Multiple Deprivation - IMD) and geographic classifications (Rural-Urban Classification - RUC). By unifying these disparate datasets, we enable analysts and business users to answer critical real estate queries:
- How do property prices and transaction volumes vary across different property types, new builds, and along the rural-urban spectrum?
- To what extent do local neighborhood deprivation factors (e.g., crime rates, education quality, living environment) influence housing valuations?
- What are the long-term trends in property sales when evaluated against neighborhood quality and geographic density?

---

## Medallion Architecture & Kimball Dimensional Modeling

The `bgd_dbt` project organizes the raw datasets into a standard Medallion Architecture (Bronze -> Silver -> Gold), culminating in a Kimball Star Schema optimized for Business Intelligence and dashboarding.

### 1. Silver Layer (Staging & Cleansing)
- **`silver_ppd` (Property Price Data):** Normalizes postcodes, appends processing timestamps, and runs incrementally to efficiently process over 30M+ historical rows without rewriting the entire table.
- **`silver_onspd` (ONS Postcode Directory):** Deduplicates postcodes by removing outdated entries. Extracts essential coordinates alongside LSOA codes (both 2011 and 2021 versions required for data bridging).
- **`silver_ruc` (Rural-Urban Classification):** Standardizes local area rural/urban string attributes.
- **`silver_imd` (Index of Multiple Deprivation):** Cleanses and transforms strict string matrices into robust numeric scores and deciles across crucial domains like income, employment, education, health, and crime.

### 2. Gold Layer (Kimball Star Schema)
- **`dim_geography` (Dimension):** The central geography hub constructed by joining `silver_onspd`, `silver_ruc`, and `silver_imd` heavily on LSOA codes. A single postcode immediately resolves to mapped coordinates, administrative regions, rural/urban settings, and IMD deprivation scores.
- **`dim_property` (Dimension):** Captures unique combinations of property types, new build status, and tenures to avoid redundancy in the fact table.
- **`dim_date` (Dimension):** A standard Kimball Date dimension spine (1990 to 2030) allowing for dynamic date-part slicing.
- **`fact_sales` (Fact):** The incremental transaction backbone utilizing `MD5` surrogate hashes and strict Date/Geography foreign keys.
- **`mart_rural_urban_stats` (Data Mart):** A heavily denormalized aggregate view fusing facts with domains. Directly answers the analytical goal by calculating average prices, total volumes, and aggregated multi-dimensional metrics (e.g., average local crime severity) grouped by precise temporal and geographic factors.

---

## Running the Project

This project leverages Docker to orchestrate a PostgreSQL data warehouse effortlessly.

### 1. Start the Database
Start the database in detached mode. Custom initialization scripts (`/docker-entrypoint-initdb.d/`) will automatically build the `raw` Bronze schema by looping through and loading the large CSV files found in the `Data/` directory.

```bash
docker compose up -d
```
*(You can monitor the database using pgAdmin located at `http://localhost:5050` with login `admin@admin.com` / `admin`).*

### 2. Run the dbt Transformations
If you do not have `dbt` natively installed on your machine, you can launch the pipeline inside an ephemeral Python Docker container connected to the database's internal network:

```bash
# Launch a throwing Python 3.10 container linked to your local bgd_default network
docker run --rm --network bgd_default -v $(pwd)/bgd_dbt:/usr/app -w /usr/app python:3.10-slim \
  /bin/bash -c "pip install dbt-postgres==1.8.2 && dbt deps && dbt run --profiles-dir ./docker_profiles --full-refresh"
```
*(Note: Because this container operates identically to the database, `docker_profiles/profiles.yml` targets `host: postgres_db` directly).*

### 3. Graceful Shutdown
```bash
docker compose down
```
