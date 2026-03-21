# bgd_dbt Project Analysis: Kimball Model & Data Transformations

The `bgd_dbt` project organizes the UK Property Price data (PPD), ONS Postcode Directory (ONSPD), and Rural-Urban Classification (RUC) using a standard **Medallion Architecture (Raw -> Silver -> Gold)** heavily integrated with **Kimball Dimensional Modeling** at the Gold tier.

## 1. Silver Layer (Staging & Cleansing)
The **Silver** schema prepares, cleans, and standardizes the raw sources.

### `silver_ppd` (Property Price Data)
- **Transformations:** 
  - Normalizes the `postcode` by stripping spaces and converting to uppercase (`REPLACE(UPPER(postcode), ' ', '')`). This normalization is critical as it serves as the key for joining with geographical data later.
  - Adds a `dbt_updated_at` timestamp.
- **Loading Strategy:** Materialized incrementally. It uses the `transaction_id` as a `unique_key` to merge (upsert) new and modified property sales.

### `silver_onspd` (ONS Postcode Directory)
- **Transformations:**
  - Performs the exact same postcode normalization as `silver_ppd` (`normalized_postcode`).
  - **Deduplication:** A single postcode might have multiple historical entries. This model dedupes by prioritizing active postcodes (where `doterm` is null or empty) and selecting the most recent introduction date (`dointr`).
  - Distills only necessary columns like `lsoa21cd` (Lower Layer Super Output Area), coordinate mappings (`latitude`, `longitude`), and district codes.

### `silver_ruc` (Rural-Urban Classification)
- **Transformations:**
  - Primarily trims whitespace from string columns (`lsoa21nm`, `ruc21cd`, `urban_rural_flag`) to prevent join failures downstream.

---

## 2. Gold Layer (Kimball Star Schema)
The **Gold** schema transforms the structured silver facts into a classic Kimball Star Schema, optimized for analytics and BI presentation.

### `dim_geography` (Dimension Table)
- **Grain:** One row per normalized postcode.
- **Implementation:** Forms the core geography dimension by joining `silver_onspd` and `silver_ruc` heavily using the **LSOA 2021 Code (`lsoa21cd`)**.
- **Result:** Any fact associated with a postcode instantly gains access to coordinates (`latitude`, `longitude`), administrative codes, and whether that postcode is classified as Rural or Urban (`urban_rural_flag`).

### `fact_sales` (Fact Table)
- **Grain:** One row per property transaction.
- **Implementation:** Built over `silver_ppd` incrementally. 
- **Columns:** 
  - **Measures:** `price`, `transfer_date`
  - **Surrogate / Foreign Keys:** `normalized_postcode` (links directly to `dim_geography`)
  - **Degenerate Dimensions:** `property_type`, `new_build`, `tenure`, `category_type` are kept in the fact table as transactional attributes.

### `mart_rural_urban_stats` (Data Mart / Aggregated View)
- This model brings the dimensional model together into an easily queryable aggregate.
- **Transformation:** Joins `fact_sales` with `dim_geography` on `normalized_postcode`. 
- **Aggregations:** It groups by `sale_year` (extracted from `transfer_date`), `urban_rural_flag`, `property_type`, and `new_build`.
- **Measures computed:** `total_transactions`, `total_sales_volume`, `average_price`, `min_price`, and `max_price`. 
- This mart is designed to directly answer business questions about housing market behaviors in urban vs. rural settings out-of-the-box.

---

## 3. Running the Project with Docker

This project includes a `docker-compose.yml` file to spin up a local PostgreSQL data warehouse and pgAdmin for exploration. The initial raw data is loaded automatically into the `raw` schema using initialization scripts located in `docker-entrypoint-initdb.d/`.

### Prerequisites
- Docker and Docker Compose installed.
- Raw CSV/data files must be correctly placed in the `Data/` directory as mapped in the compose volumes.

### Steps to Run

1. **Start the Database and pgAdmin** in detached mode:
   ```bash
   docker compose up -d
   ```
   *(Note: The first startup may take a moment while the initialization scripts import the heavy raw data into Postgres).*

2. **Access pgAdmin (Database UI)**:
   - **URL**: [http://localhost:5050](http://localhost:5050)
   - **Email**: `admin@admin.com`
   - **Password**: `admin`

3. **Direct Database Credentials** (for dbt or IDE connections):
   - **Host**: `localhost`
   - **Port**: `5432`
   - **User**: `myuser`
   - **Password**: `mypassword`
   - **Database**: `mydatabase`

4. **Run the dbt Pipeline**:
   Once the database is up and the raw data is loaded, navigate to the dbt project and trigger the transformations (Silver -> Gold):
   ```bash
   cd bgd_dbt
   dbt deps
   dbt run
   ```

5. **Stop the environment**:
   ```bash
   docker compose down
   ```
