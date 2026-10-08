# Databricks notebook source
# DBTITLE 1,📘 Notebook Header - Phase 12 Day 44
# MAGIC %md
# MAGIC # 🚀 Data Engineering Training — Phase 12 Day 44  
# MAGIC ## 🏗 End-to-End Data Engineering Project (Lakehouse Implementation)  
# MAGIC
# MAGIC ### 🎯 Topics Covered:
# MAGIC - End-to-End Pipeline Design  
# MAGIC - Ingestion (Auto Loader)  
# MAGIC - Processing (PySpark Transformations)  
# MAGIC - Storage (Delta Lake)  
# MAGIC - Medallion Architecture  
# MAGIC - Governance (Unity Catalog)  
# MAGIC - Orchestration (Workflows + ADF Integration)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👨‍💻 Author: TRRaveendra  
# MAGIC ### 🏷️ Watermark: **@TRRaveendra**  
# MAGIC ### ⚙️ Platform: Databricks (Serverless + Unity Catalog + Delta Lake + Streaming + Workflows + ADF)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📌 Objective:
# MAGIC Build a complete production-grade data pipeline covering ingestion, processing, storage, governance, and orchestration using Databricks Lakehouse architecture.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Engineering Constraints:
# MAGIC - ✅ Use Databricks Serverless Compute
# MAGIC - ✅ Use Unity Catalog for all tables and volumes
# MAGIC - ❌ DO NOT use RDDs
# MAGIC - ❌ DO NOT use cache() / persist()
# MAGIC - ❌ DO NOT use /tmp or local storage
# MAGIC - ✅ Use `_metadata.file_path` for file tracking
# MAGIC - ✅ Follow Medallion + incremental + governed design

# COMMAND ----------

# DBTITLE 1,📊 Section 1 - Project Overview
# MAGIC %md
# MAGIC ## 📊 SECTION 1 — Project Overview
# MAGIC
# MAGIC ### 🎯 Business Use Case: E-Commerce Sales Analytics Platform
# MAGIC
# MAGIC **Scenario:**  
# MAGIC We're building a real-time analytics platform for an e-commerce company that processes:
# MAGIC - Customer orders (JSON files)
# MAGIC - Product catalog updates
# MAGIC - Inventory changes
# MAGIC - User clickstream events
# MAGIC
# MAGIC The platform needs to:
# MAGIC 1. Ingest data continuously from cloud storage
# MAGIC 2. Clean and validate data
# MAGIC 3. Create aggregated analytics tables
# MAGIC 4. Power BI dashboards and ML models
# MAGIC 5. Ensure data governance and compliance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧒 ELI5 Explanation:
# MAGIC Imagine you have a huge online store. Every second, customers are buying products, browsing items, and leaving reviews. All this information comes in as files (like receipts) that land in a folder. 
# MAGIC
# MAGIC Our job is to:
# MAGIC - **Pick up** these files automatically (Auto Loader)
# MAGIC - **Organize** them neatly (Bronze layer - raw data)
# MAGIC - **Clean and fix** any mistakes (Silver layer - cleaned data)
# MAGIC - **Summarize** the important stuff like "How many sales today?" (Gold layer - analytics)
# MAGIC - **Make sure** only the right people can see sensitive data (Governance)
# MAGIC - **Schedule** everything to run automatically (Orchestration)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏛️ Architect-Level Explanation:
# MAGIC This is a **Lambda Architecture** implementation using Databricks Lakehouse:
# MAGIC
# MAGIC **Key Components:**
# MAGIC 1. **Ingestion Layer**: Auto Loader with schema inference and evolution
# MAGIC 2. **Storage Layer**: Delta Lake with ACID transactions
# MAGIC 3. **Processing Layer**: Spark Structured Streaming for real-time + batch processing
# MAGIC 4. **Medallion Architecture**: Bronze (raw) → Silver (curated) → Gold (aggregated)
# MAGIC 5. **Governance Layer**: Unity Catalog for metadata, lineage, and access control
# MAGIC 6. **Orchestration Layer**: Databricks Workflows + Azure Data Factory
# MAGIC
# MAGIC **Design Principles:**
# MAGIC - **Incremental Processing**: Process only new/changed data
# MAGIC - **Idempotency**: Safe to re-run pipelines
# MAGIC - **Schema Evolution**: Handle schema changes gracefully
# MAGIC - **Cost Optimization**: Serverless compute with auto-scaling
# MAGIC - **Data Quality**: Built-in validation and expectations

# COMMAND ----------

# DBTITLE 1,🏛️ Section 2 - Architecture Design
# MAGIC %md
# MAGIC ## 🏛️ SECTION 2 — Architecture Design
# MAGIC
# MAGIC ### 📊 End-to-End Data Flow:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────┐
# MAGIC │  Data Sources      │
# MAGIC │  (JSON/CSV/Parquet)│
# MAGIC │  S3 / ADLS / GCS   │
# MAGIC └───────┬─────────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴──────────┐
# MAGIC │  AUTO LOADER       │
# MAGIC │  (cloudFiles)      │
# MAGIC │  Schema Inference  │
# MAGIC └────────┬──────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴──────────┐
# MAGIC │  BRONZE LAYER      │
# MAGIC │  (Raw Data)        │
# MAGIC │  Delta Table       │
# MAGIC │  + Metadata        │
# MAGIC └────────┬──────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴──────────┐
# MAGIC │  SILVER LAYER      │
# MAGIC │  (Cleaned)         │
# MAGIC │  Validated         │
# MAGIC │  Deduplicated      │
# MAGIC │  Business Logic    │
# MAGIC └────────┬──────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴──────────┐
# MAGIC │  GOLD LAYER        │
# MAGIC │  (Aggregated)      │
# MAGIC │  Business KPIs     │
# MAGIC │  Analytics Ready   │
# MAGIC └────────┬──────────┘
# MAGIC          │
# MAGIC          ↓
# MAGIC ┌────────┴──────────┐
# MAGIC │  CONSUMPTION       │
# MAGIC │  BI Dashboards     │
# MAGIC │  ML Models         │
# MAGIC │  APIs / Reports    │
# MAGIC └────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔒 Governance Layer (Unity Catalog):
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog Hierarchy:
# MAGIC
# MAGIC training_catalog
# MAGIC   ├── ecommerce_schema
# MAGIC   │   ├── bronze_orders (table)
# MAGIC   │   ├── silver_orders (table)
# MAGIC   │   ├── gold_sales_summary (table)
# MAGIC   │   └── project_volume (volume)
# MAGIC   │       ├── raw/ (landing zone)
# MAGIC   │       ├── checkpoints/
# MAGIC   │       └── archives/
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔄 Orchestration Layer:
# MAGIC
# MAGIC **Databricks Workflows:**
# MAGIC - Task 1: Ingest raw data (Auto Loader)
# MAGIC - Task 2: Process Bronze → Silver
# MAGIC - Task 3: Process Silver → Gold
# MAGIC - Task 4: Run data quality checks
# MAGIC - Task 5: Refresh BI dashboards
# MAGIC
# MAGIC **Azure Data Factory (Enterprise):**
# MAGIC - Schedule: Hourly / Daily / Event-driven
# MAGIC - Monitoring: Alerts and notifications
# MAGIC - Integration: Multi-source orchestration

# COMMAND ----------

# DBTITLE 1,📥 Section 3 - Ingestion Layer
# MAGIC %md
# MAGIC ## 📥 SECTION 3 — Ingestion Layer (Auto Loader)
# MAGIC
# MAGIC ### 🤖 What is Auto Loader?
# MAGIC
# MAGIC **ELI5:**  
# MAGIC Auto Loader is like a smart robot that watches a folder. Whenever new files appear, it automatically picks them up and processes them. It remembers which files it already processed, so it never does the same work twice!
# MAGIC
# MAGIC **Architect View:**  
# MAGIC Auto Loader (`cloudFiles`) is Databricks' optimized file ingestion service that:
# MAGIC - **Automatically** discovers new files in cloud storage (S3, ADLS, GCS)
# MAGIC - **Incrementally** processes only new data
# MAGIC - **Infers** schema automatically with evolution support
# MAGIC - **Scales** efficiently using file notification (preferred) or directory listing
# MAGIC - **Tracks** processed files using RocksDB checkpoints
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key Features:
# MAGIC
# MAGIC 1. **Schema Inference**: Automatically detects column types
# MAGIC 2. **Schema Evolution**: Handles new columns gracefully
# MAGIC 3. **Exactly-Once Processing**: No duplicates, no data loss
# MAGIC 4. **Format Support**: JSON, CSV, Parquet, Avro, ORC, text
# MAGIC 5. **Metadata Tracking**: `_metadata.file_path`, `_metadata.file_modification_time`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Hands-On: Setup and Configuration

# COMMAND ----------

# DBTITLE 1,⚙️ Setup - Define Catalog, Schema, Volume
# ===================================================================
# SETUP: Define Unity Catalog Resources
# ===================================================================

# Unity Catalog hierarchy
CATALOG = "training_catalog"
SCHEMA = "ecommerce_schema"
VOLUME = "project_volume"

# Construct paths
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_DATA_PATH = f"{VOLUME_PATH}/raw/orders"
CHECKPOINT_PATH = f"{VOLUME_PATH}/checkpoints"

print(f"✅ Catalog: {CATALOG}")
print(f"✅ Schema: {SCHEMA}")
print(f"✅ Volume: {VOLUME}")
print(f"✅ Raw Data Path: {RAW_DATA_PATH}")
print(f"✅ Checkpoint Path: {CHECKPOINT_PATH}")

# COMMAND ----------

# DBTITLE 1,🏛️ Create Unity Catalog Objects
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- Create Unity Catalog: Catalog, Schema, Volume
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Create catalog (if not exists)
# MAGIC CREATE CATALOG IF NOT EXISTS training_catalog;
# MAGIC
# MAGIC -- Use catalog
# MAGIC USE CATALOG training_catalog;
# MAGIC
# MAGIC -- Create schema
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce_schema
# MAGIC COMMENT 'E-commerce sales analytics schema - Phase 12 Day 44 @TRRaveendra';
# MAGIC
# MAGIC -- Use schema
# MAGIC USE SCHEMA ecommerce_schema;
# MAGIC
# MAGIC -- Create volume for data storage
# MAGIC CREATE VOLUME IF NOT EXISTS project_volume
# MAGIC COMMENT 'Project volume for raw data, checkpoints, and archives';
# MAGIC
# MAGIC -- Verify
# MAGIC SHOW VOLUMES IN ecommerce_schema;

# COMMAND ----------

# DBTITLE 1,📄 Generate Sample Data (Simulating Landing Zone)
# ===================================================================
# Generate Sample E-Commerce Orders Data
# Simulates files landing in cloud storage
# ===================================================================

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json

# Create sample orders data
orders_data = [
    {"order_id": "ORD001", "customer_id": "CUST101", "product_id": "PROD501", "product_name": "Laptop", "category": "Electronics", "quantity": 1, "price": 1200.00, "order_date": "2026-04-21", "status": "completed"},
    {"order_id": "ORD002", "customer_id": "CUST102", "product_id": "PROD502", "product_name": "Mouse", "category": "Electronics", "quantity": 2, "price": 25.50, "order_date": "2026-04-21", "status": "completed"},
    {"order_id": "ORD003", "customer_id": "CUST103", "product_id": "PROD503", "product_name": "Desk Chair", "category": "Furniture", "quantity": 1, "price": 350.00, "order_date": "2026-04-21", "status": "pending"},
    {"order_id": "ORD004", "customer_id": "CUST104", "product_id": "PROD504", "product_name": "Monitor", "category": "Electronics", "quantity": 1, "price": 450.00, "order_date": "2026-04-21", "status": "completed"},
    {"order_id": "ORD005", "customer_id": "CUST105", "product_id": "PROD505", "product_name": "Keyboard", "category": "Electronics", "quantity": 3, "price": 75.00, "order_date": "2026-04-21", "status": "completed"},
    {"order_id": "ORD006", "customer_id": "CUST106", "product_id": "PROD506", "product_name": "Desk Lamp", "category": "Furniture", "quantity": 2, "price": 45.00, "order_date": "2026-04-21", "status": "shipped"},
    {"order_id": "ORD007", "customer_id": "CUST107", "product_id": "PROD507", "product_name": "Notebook", "category": "Stationery", "quantity": 10, "price": 3.50, "order_date": "2026-04-21", "status": "completed"},
    {"order_id": "ORD008", "customer_id": "CUST108", "product_id": "PROD508", "product_name": "Webcam", "category": "Electronics", "quantity": 1, "price": 120.00, "order_date": "2026-04-21", "status": "completed"},
]

# Create DataFrame
df_sample = spark.createDataFrame(orders_data)

# Write sample data as JSON files to volume (simulating landing zone)
df_sample.write.mode("overwrite").format("json").save(f"{RAW_DATA_PATH}/batch_001")

print(f"✅ Sample data written to: {RAW_DATA_PATH}/batch_001")
print(f"   Total records: {df_sample.count()}")

# Display sample
display(df_sample)

# COMMAND ----------

# DBTITLE 1,🚀 Auto Loader - Read Stream with Schema Inference
# ===================================================================
# AUTO LOADER: Read Stream from Cloud Storage
# Uses cloudFiles format with schema inference
# ===================================================================

from pyspark.sql.functions import col, current_timestamp

# Define Auto Loader stream
df_raw_stream = (spark.readStream
    .format("cloudFiles")  # Auto Loader format
    .option("cloudFiles.format", "json")  # Source file format
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema_bronze")  # Schema evolution tracking
    .option("cloudFiles.inferColumnTypes", "true")  # Infer data types
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle schema changes
    .load(RAW_DATA_PATH)  # Source path
)

# Add metadata columns for tracking
df_raw_enriched = df_raw_stream \
    .withColumn("source_file", col("_metadata.file_path")) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("file_modification_time", col("_metadata.file_modification_time"))

print("✅ Auto Loader stream configured successfully!")
print("   - Schema inference: ENABLED")
print("   - Schema evolution: addNewColumns")
print("   - Metadata tracking: source_file, ingestion_timestamp")
print("\n📋 Stream Schema:")
df_raw_enriched.printSchema()

# COMMAND ----------

# DBTITLE 1,🥉 Section 4 - Bronze Layer
# MAGIC %md
# MAGIC ## 🥉 SECTION 4 — Bronze Layer (Raw Data Persistence)
# MAGIC
# MAGIC ### 🎯 Purpose of Bronze Layer:
# MAGIC
# MAGIC **ELI5:**  
# MAGIC The Bronze layer is like a safe deposit box. When files arrive, we immediately save them without changing anything. This way, we always have the original data if something goes wrong later.
# MAGIC
# MAGIC **Architect View:**  
# MAGIC The Bronze layer serves as the **immutable landing zone** for raw data:
# MAGIC - **Preserves** original data exactly as received
# MAGIC - **Enables** replay and reprocessing
# MAGIC - **Tracks** source lineage (file path, timestamps)
# MAGIC - **Stores** in Delta format for ACID guarantees
# MAGIC - **Supports** streaming and batch reads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Bronze Layer Characteristics:
# MAGIC
# MAGIC * **Schema**: Flexible, matches source data + metadata
# MAGIC * **Format**: Delta Lake (ACID transactions)
# MAGIC * **Partitioning**: Optional (by date for time-series data)
# MAGIC * **Quality**: No validation, accept all data
# MAGIC * **Retention**: Long-term (compliance, audit)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Hands-On: Write to Bronze Table

# COMMAND ----------

# DBTITLE 1,🔄 Write Stream to Bronze Delta Table
# ===================================================================
# BRONZE LAYER: Write raw stream to Delta table
# Uses Unity Catalog managed table
# ===================================================================

# Define Bronze table name
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
BRONZE_CHECKPOINT = f"{CHECKPOINT_PATH}/bronze_orders"

print(f"📦 Writing to Bronze table: {BRONZE_TABLE}")
print(f"💾 Checkpoint location: {BRONZE_CHECKPOINT}")

# Write stream to Delta table
query_bronze = (df_raw_enriched.writeStream
    .format("delta")  # Delta Lake format
    .outputMode("append")  # Append new records
    .option("checkpointLocation", BRONZE_CHECKPOINT)  # Exactly-once processing
    .option("mergeSchema", "true")  # Handle schema evolution
    .trigger(availableNow=True)  # Process all available data then stop
    .toTable(BRONZE_TABLE)  # Unity Catalog table
)

print("\n⏳ Processing stream...")
query_bronze.awaitTermination()
print("✅ Bronze layer ingestion completed!")

# COMMAND ----------

# DBTITLE 1,🔍 Verify Bronze Table Data
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- VERIFY: Check Bronze table contents
# MAGIC -- ===================================================================
# MAGIC
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     quantity,
# MAGIC     price,
# MAGIC     order_date,
# MAGIC     status,
# MAGIC     source_file,
# MAGIC     ingestion_timestamp
# MAGIC FROM training_catalog.ecommerce_schema.bronze_orders
# MAGIC ORDER BY ingestion_timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,🧼 Section 5 - Silver Layer
# MAGIC %md
# MAGIC ## 🧼 SECTION 5 — Silver Layer (Cleaned & Validated Data)
# MAGIC
# MAGIC ### 🎯 Purpose of Silver Layer:
# MAGIC
# MAGIC **ELI5:**  
# MAGIC The Silver layer is where we clean up the messy data. We:
# MAGIC - Fix typos and formatting
# MAGIC - Remove duplicate entries
# MAGIC - Check if the data makes sense (like prices can't be negative)
# MAGIC - Organize it in a way that's easy to use
# MAGIC
# MAGIC **Architect View:**  
# MAGIC The Silver layer implements **data quality and business logic**:
# MAGIC - **Cleanses** data (null handling, formatting)
# MAGIC - **Validates** business rules (constraints, ranges)
# MAGIC - **Deduplicates** records
# MAGIC - **Standardizes** formats (dates, names)
# MAGIC - **Enriches** with derived columns
# MAGIC - **Conforms** to enterprise data model
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Silver Layer Transformations:
# MAGIC
# MAGIC 1. **Data Type Casting**: Ensure correct types
# MAGIC 2. **Null Handling**: Replace or filter nulls
# MAGIC 3. **Deduplication**: Remove duplicate order_ids
# MAGIC 4. **Validation**: Check business rules
# MAGIC 5. **Standardization**: Consistent formatting
# MAGIC 6. **Enrichment**: Add calculated fields
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Hands-On: Bronze to Silver Processing

# COMMAND ----------

# DBTITLE 1,📥 Read Bronze Layer as Stream
# ===================================================================
# Read Bronze layer as streaming source
# ===================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read Bronze table as stream
df_bronze_stream = spark.readStream \
    .format("delta") \
    .table(BRONZE_TABLE)

print(f"✅ Reading from Bronze table: {BRONZE_TABLE}")
print(f"   Schema: {len(df_bronze_stream.schema)} columns")

# COMMAND ----------

# DBTITLE 1,✨ Apply Silver Layer Transformations
# ===================================================================
# SILVER LAYER: Data Cleaning & Validation
# ===================================================================

from pyspark.sql.functions import *

# Step 1: Data cleansing and type casting
df_cleansed = df_bronze_stream \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("price") > 0) \
    .filter(col("quantity") > 0) \
    .withColumn("price", col("price").cast("decimal(10,2)")) \
    .withColumn("quantity", col("quantity").cast("integer")) \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("category", upper(trim(col("category")))) \
    .withColumn("status", lower(trim(col("status"))))

# Step 2: Add business logic - calculate total amount
df_enriched = df_cleansed \
    .withColumn("total_amount", col("price") * col("quantity"))

# Step 3: Deduplication using dropDuplicates (streaming-compatible)
# Use watermark to handle late-arriving data and deduplicate by order_id
df_silver = df_enriched \
    .withWatermark("ingestion_timestamp", "1 hour") \
    .dropDuplicates(["order_id"]) \
    .withColumn("silver_processed_timestamp", current_timestamp())

print("✅ Silver layer transformations applied:")
print("   - Null filtering")
print("   - Data type casting")
print("   - Business logic (total_amount)")
print("   - Deduplication with watermark")
print("   - Standardization (upper/lower case)")
print("\n📋 Silver Schema:")
df_silver.printSchema()

# COMMAND ----------

# DBTITLE 1,💾 Write to Silver Delta Table
# ===================================================================
# Write Silver layer to Delta table
# ===================================================================

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
SILVER_CHECKPOINT = f"{CHECKPOINT_PATH}/silver_orders"

print(f"📦 Writing to Silver table: {SILVER_TABLE}")
print(f"💾 Checkpoint location: {SILVER_CHECKPOINT}")

# Write stream to Delta table
query_silver = (df_silver.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

print("\n⏳ Processing Silver layer...")
query_silver.awaitTermination()
print("✅ Silver layer processing completed!")

# COMMAND ----------

# DBTITLE 1,🔍 Verify Silver Table Data
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- VERIFY: Check Silver table with calculated fields
# MAGIC -- ===================================================================
# MAGIC
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     quantity,
# MAGIC     price,
# MAGIC     total_amount,  -- Calculated field
# MAGIC     order_date,
# MAGIC     status,
# MAGIC     silver_processed_timestamp
# MAGIC FROM training_catalog.ecommerce_schema.silver_orders
# MAGIC ORDER BY total_amount DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,🥇 Section 6 - Gold Layer
# MAGIC %md
# MAGIC ## 🥇 SECTION 6 — Gold Layer (Business-Ready Analytics)
# MAGIC
# MAGIC ### 🎯 Purpose of Gold Layer:
# MAGIC
# MAGIC **ELI5:**  
# MAGIC The Gold layer is where we answer business questions like:
# MAGIC - "How much did we sell today?"
# MAGIC - "Which products are most popular?"
# MAGIC - "Who are our top customers?"
# MAGIC
# MAGIC We take the cleaned data and create summary tables that are super fast to read.
# MAGIC
# MAGIC **Architect View:**  
# MAGIC The Gold layer provides **aggregated, business-ready datasets**:
# MAGIC - **Aggregates** data for analytics (SUM, COUNT, AVG)
# MAGIC - **Optimizes** for query performance
# MAGIC - **Denormalizes** for BI tool consumption
# MAGIC - **Serves** dashboards, reports, and ML models
# MAGIC - **Implements** business KPIs and metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Gold Layer Characteristics:
# MAGIC
# MAGIC * **Schema**: Star/snowflake schema, highly denormalized
# MAGIC * **Granularity**: Aggregated (daily, monthly, by category)
# MAGIC * **Performance**: Optimized with Z-ordering, liquid clustering
# MAGIC * **Audience**: Business analysts, data scientists, BI tools
# MAGIC * **Updates**: Incremental aggregation or full refresh
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Common Gold Layer Tables:
# MAGIC
# MAGIC 1. **Sales Summary by Date**
# MAGIC 2. **Product Performance by Category**
# MAGIC 3. **Customer Purchase Patterns**
# MAGIC 4. **Inventory Levels**
# MAGIC 5. **Revenue Forecasting Base**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Hands-On: Create Gold Layer Aggregations

# COMMAND ----------

# DBTITLE 1,📥 Read Silver Layer for Aggregation
# ===================================================================
# Read Silver layer for Gold aggregations
# ===================================================================

# Read Silver table as stream
df_silver_stream = spark.readStream \
    .format("delta") \
    .table(SILVER_TABLE)

print(f"✅ Reading from Silver table: {SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,📈 Gold Aggregation 1: Sales Summary by Category
# ===================================================================
# GOLD AGGREGATION: Sales summary by category and date
# ===================================================================

from pyspark.sql.functions import *

# Aggregate: Total sales, order count, avg price by category and date
df_gold_category_sales = df_silver_stream \
    .groupBy("category", "order_date") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value"),
        sum("quantity").alias("total_units_sold"),
        approx_count_distinct("customer_id").alias("unique_customers")  # Use approx for streaming
    ) \
    .withColumn("gold_processed_timestamp", current_timestamp())

print("✅ Gold aggregation created: Sales by Category")
print("   Metrics: total_orders, total_revenue, avg_order_value, total_units_sold, unique_customers")

# COMMAND ----------

# DBTITLE 1,💾 Write Gold Table 1: Category Sales Summary
# ===================================================================
# Write Gold table: Category sales summary
# ===================================================================

GOLD_TABLE_CATEGORY = f"{CATALOG}.{SCHEMA}.gold_sales_by_category"
GOLD_CHECKPOINT_CATEGORY = f"{CHECKPOINT_PATH}/gold_sales_category"

print(f"📦 Writing to Gold table: {GOLD_TABLE_CATEGORY}")

# Write with complete mode for aggregations
query_gold_category = (df_gold_category_sales.writeStream
    .format("delta")
    .outputMode("complete")  # Complete mode for aggregations
    .option("checkpointLocation", GOLD_CHECKPOINT_CATEGORY)
    .trigger(availableNow=True)
    .toTable(GOLD_TABLE_CATEGORY)
)

print("⏳ Processing Gold layer (Category Sales)...")
query_gold_category.awaitTermination()
print("✅ Gold layer (Category Sales) completed!")

# COMMAND ----------

# DBTITLE 1,📉 Gold Aggregation 2: Customer Purchase Summary
# ===================================================================
# GOLD AGGREGATION: Customer purchase patterns
# Using batch processing for this aggregation
# ===================================================================

# Read Silver as batch
df_silver_batch = spark.read.format("delta").table(SILVER_TABLE)

# Aggregate: Customer lifetime value and purchase frequency
df_gold_customer = df_silver_batch \
    .groupBy("customer_id") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_amount").alias("lifetime_value"),
        avg("total_amount").alias("avg_order_value"),
        max("order_date").alias("last_order_date"),
        min("order_date").alias("first_order_date"),
        countDistinct("category").alias("categories_purchased")
    ) \
    .withColumn("customer_segment", 
        when(col("lifetime_value") >= 1000, "Premium")
        .when(col("lifetime_value") >= 500, "Gold")
        .when(col("lifetime_value") >= 100, "Silver")
        .otherwise("Bronze")
    ) \
    .withColumn("gold_processed_timestamp", current_timestamp())

print("✅ Gold aggregation created: Customer Purchase Summary")
print("   Metrics: lifetime_value, total_orders, customer_segment")

# COMMAND ----------

# DBTITLE 1,💾 Write Gold Table 2: Customer Summary
# ===================================================================
# Write Gold table: Customer summary (batch write)
# ===================================================================

GOLD_TABLE_CUSTOMER = f"{CATALOG}.{SCHEMA}.gold_customer_summary"

print(f"📦 Writing to Gold table: {GOLD_TABLE_CUSTOMER}")

# Write as batch (overwrite mode for this demo)
df_gold_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD_TABLE_CUSTOMER)

print(f"✅ Gold table written: {GOLD_TABLE_CUSTOMER}")
print(f"   Total customers: {df_gold_customer.count()}")

# Display sample
display(df_gold_customer.orderBy(col("lifetime_value").desc()))

# COMMAND ----------

# DBTITLE 1,🔍 Verify Gold Tables - Category Sales
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- VERIFY: Gold table - Sales by Category
# MAGIC -- ===================================================================
# MAGIC
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     order_date,
# MAGIC     total_orders,
# MAGIC     total_revenue,
# MAGIC     ROUND(avg_order_value, 2) as avg_order_value,
# MAGIC     total_units_sold,
# MAGIC     unique_customers
# MAGIC FROM training_catalog.ecommerce_schema.gold_sales_by_category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,🔍 Verify Gold Tables - Customer Summary
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- VERIFY: Gold table - Customer Summary with Segmentation
# MAGIC -- ===================================================================
# MAGIC
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     total_orders,
# MAGIC     ROUND(lifetime_value, 2) as lifetime_value,
# MAGIC     ROUND(avg_order_value, 2) as avg_order_value,
# MAGIC     customer_segment,
# MAGIC     categories_purchased,
# MAGIC     first_order_date,
# MAGIC     last_order_date
# MAGIC FROM training_catalog.ecommerce_schema.gold_customer_summary
# MAGIC ORDER BY lifetime_value DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,🔒 Section 7 - Governance with Unity Catalog
# MAGIC %md
# MAGIC ## 🔒 SECTION 7 — Governance (Unity Catalog)
# MAGIC
# MAGIC ### 🎯 What is Unity Catalog?
# MAGIC
# MAGIC **ELI5:**  
# MAGIC Unity Catalog is like a library system for your data. It:
# MAGIC - Keeps track of where all your data is
# MAGIC - Decides who can see what
# MAGIC - Remembers where data came from
# MAGIC - Makes sure everyone uses the same names for things
# MAGIC
# MAGIC **Architect View:**  
# MAGIC Unity Catalog is Databricks' **unified governance solution** providing:
# MAGIC - **Fine-grained access control** (table, column, row-level)
# MAGIC - **Data lineage** (upstream/downstream dependencies)
# MAGIC - **Data discovery** (search, tagging, documentation)
# MAGIC - **Audit logging** (who accessed what, when)
# MAGIC - **Cross-workspace** governance
# MAGIC - **Delta Sharing** for secure data sharing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Unity Catalog Hierarchy:
# MAGIC
# MAGIC ```
# MAGIC Metastore (Account-level)
# MAGIC   │
# MAGIC   ├── Catalog (training_catalog)
# MAGIC   │     │
# MAGIC   │     ├── Schema (ecommerce_schema)
# MAGIC   │     │     │
# MAGIC   │     │     ├── Tables
# MAGIC   │     │     │   ├── bronze_orders
# MAGIC   │     │     │   ├── silver_orders
# MAGIC   │     │     │   ├── gold_sales_by_category
# MAGIC   │     │     │   └── gold_customer_summary
# MAGIC   │     │     │
# MAGIC   │     │     ├── Views
# MAGIC   │     │     │
# MAGIC   │     │     └── Volumes
# MAGIC   │     │         └── project_volume
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🛡️ Key Governance Features:
# MAGIC
# MAGIC 1. **Access Control Lists (ACLs)**:
# MAGIC    - Catalog-level permissions
# MAGIC    - Schema-level permissions
# MAGIC    - Table/view-level permissions
# MAGIC    - Column-level masking
# MAGIC    - Row-level filtering
# MAGIC
# MAGIC 2. **Data Lineage**:
# MAGIC    - Table dependencies
# MAGIC    - Column-level lineage
# MAGIC    - Notebook/query tracking
# MAGIC
# MAGIC 3. **Tagging & Documentation**:
# MAGIC    - Business glossary
# MAGIC    - Technical metadata
# MAGIC    - Custom tags (PII, sensitive)
# MAGIC
# MAGIC 4. **Audit Logging**:
# MAGIC    - Access logs
# MAGIC    - Change history
# MAGIC    - Compliance reports
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Hands-On: Governance Operations

# COMMAND ----------

# DBTITLE 1,📋 View Catalog Metadata
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- GOVERNANCE: View all tables in schema
# MAGIC -- ===================================================================
# MAGIC
# MAGIC SHOW TABLES IN training_catalog.ecommerce_schema;

# COMMAND ----------

# DBTITLE 1,📖 Add Table Comments (Documentation)
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- GOVERNANCE: Add table and column comments
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Add table-level comment
# MAGIC COMMENT ON TABLE training_catalog.ecommerce_schema.bronze_orders IS 
# MAGIC 'Bronze layer: Raw e-commerce orders ingested via Auto Loader. Contains unvalidated source data with metadata tracking. @TRRaveendra Phase 12 Day 44';
# MAGIC
# MAGIC COMMENT ON TABLE training_catalog.ecommerce_schema.silver_orders IS 
# MAGIC 'Silver layer: Cleaned and validated orders with business logic applied. Deduplicated and ready for analytics. @TRRaveendra Phase 12 Day 44';
# MAGIC
# MAGIC COMMENT ON TABLE training_catalog.ecommerce_schema.gold_sales_by_category IS 
# MAGIC 'Gold layer: Aggregated sales metrics by product category and date. Optimized for BI dashboards. @TRRaveendra Phase 12 Day 44';
# MAGIC
# MAGIC COMMENT ON TABLE training_catalog.ecommerce_schema.gold_customer_summary IS 
# MAGIC 'Gold layer: Customer lifetime value and segmentation. Used for customer analytics and marketing. @TRRaveendra Phase 12 Day 44';
# MAGIC
# MAGIC SELECT 'Comments added to all tables' AS status;

# COMMAND ----------

# DBTITLE 1,🔍 View Table Properties and Lineage
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- GOVERNANCE: View table details
# MAGIC -- ===================================================================
# MAGIC
# MAGIC DESCRIBE EXTENDED training_catalog.ecommerce_schema.silver_orders;

# COMMAND ----------

# DBTITLE 1,🏷️ Set Table Tags (PII Classification)
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- GOVERNANCE: Tag tables for classification
# MAGIC -- Tags help identify sensitive data and apply policies
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Set tags on tables (conceptual - requires tag creation first)
# MAGIC ALTER TABLE training_catalog.ecommerce_schema.silver_orders 
# MAGIC SET TAGS ('classification' = 'internal', 'contains_pii' = 'yes', 'data_quality' = 'validated');
# MAGIC
# MAGIC ALTER TABLE training_catalog.ecommerce_schema.gold_customer_summary 
# MAGIC SET TAGS ('classification' = 'confidential', 'contains_pii' = 'yes', 'business_critical' = 'yes');
# MAGIC
# MAGIC SELECT 'Table tags configured' AS status;

# COMMAND ----------

# DBTITLE 1,🔍 View Data Lineage (Programmatic)
# ===================================================================
# GOVERNANCE: View table lineage information
# ===================================================================

# Get table history (Delta time travel)
df_history = spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}")
print(f"📋 Table History: {SILVER_TABLE}")
display(df_history.select("version", "timestamp", "operation", "operationMetrics").limit(10))

# Show table properties
df_properties = spark.sql(f"SHOW TBLPROPERTIES {SILVER_TABLE}")
print(f"\n📊 Table Properties: {SILVER_TABLE}")
display(df_properties)

# COMMAND ----------

# DBTITLE 1,🔄 Section 8 - Orchestration
# MAGIC %md
# MAGIC ## 🔄 SECTION 8 — Orchestration (Workflows + ADF Integration)
# MAGIC
# MAGIC ### 🎯 What is Orchestration?
# MAGIC
# MAGIC **ELI5:**  
# MAGIC Orchestration is like a conductor leading an orchestra. Instead of running each step manually, we create a schedule that automatically:
# MAGIC - Runs the ingestion at 8 AM
# MAGIC - Then cleans the data
# MAGIC - Then creates summaries
# MAGIC - Then refreshes dashboards
# MAGIC - All without anyone clicking buttons!
# MAGIC
# MAGIC **Architect View:**  
# MAGIC Orchestration provides **automated workflow management**:
# MAGIC - **Schedules** pipeline execution (time-based, event-driven)
# MAGIC - **Manages** task dependencies (DAG execution)
# MAGIC - **Handles** failures and retries
# MAGIC - **Monitors** SLAs and performance
# MAGIC - **Integrates** with enterprise tools (ADF, Airflow)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Databricks Workflows (Lakeflow Jobs):
# MAGIC
# MAGIC **Features:**
# MAGIC - Multi-task jobs with dependencies
# MAGIC - Notebook, Python, JAR, SQL tasks
# MAGIC - Schedule triggers (cron, file arrival)
# MAGIC - Cluster management (job clusters, serverless)
# MAGIC - Email/webhook notifications
# MAGIC - Git integration for CI/CD
# MAGIC
# MAGIC **Example Workflow:**
# MAGIC ```
# MAGIC Task 1: Ingest Raw Data (Bronze)
# MAGIC    ↓
# MAGIC Task 2: Process to Silver
# MAGIC    ↓
# MAGIC Task 3: Aggregate to Gold
# MAGIC    ↓
# MAGIC Task 4: Data Quality Checks
# MAGIC    ↓
# MAGIC Task 5: Refresh BI Dashboard
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🏭 Azure Data Factory (ADF) Integration:
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Enterprise-wide orchestration across multiple platforms
# MAGIC - Integration with on-premises systems
# MAGIC - Complex ETL across Azure services
# MAGIC - Centralized monitoring and governance
# MAGIC
# MAGIC **ADF Pipeline Components:**
# MAGIC 1. **Linked Services**: Connections to Databricks workspace
# MAGIC 2. **Datasets**: Input/output data references
# MAGIC 3. **Pipeline**: Workflow definition
# MAGIC 4. **Triggers**: Schedule or event-based execution
# MAGIC 5. **Activities**: Databricks notebook/job execution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Orchestration Strategy:

# COMMAND ----------

# DBTITLE 1,📝 Workflow Design - Task Definition
# ===================================================================
# ORCHESTRATION: Define workflow tasks
# This is conceptual - actual implementation via Databricks UI/API
# ===================================================================

# Workflow Definition (Conceptual)
workflow_definition = {
    "name": "ecommerce_etl_pipeline",
    "schedule": "0 0 8 * * ?",  # Daily at 8 AM
    "tasks": [
        {
            "task_key": "ingest_bronze",
            "notebook_path": "/Path/To/Bronze_Ingestion",
            "cluster": "serverless",
            "timeout_seconds": 3600
        },
        {
            "task_key": "process_silver",
            "depends_on": ["ingest_bronze"],
            "notebook_path": "/Path/To/Silver_Processing",
            "cluster": "serverless",
            "timeout_seconds": 3600
        },
        {
            "task_key": "aggregate_gold",
            "depends_on": ["process_silver"],
            "notebook_path": "/Path/To/Gold_Aggregation",
            "cluster": "serverless",
            "timeout_seconds": 1800
        },
        {
            "task_key": "data_quality_check",
            "depends_on": ["aggregate_gold"],
            "notebook_path": "/Path/To/Quality_Checks",
            "cluster": "serverless",
            "timeout_seconds": 600
        }
    ],
    "email_notifications": {
        "on_failure": ["data-eng-team@company.com"],
        "on_success": ["data-eng-team@company.com"]
    },
    "max_retries": 2,
    "retry_delay_seconds": 300
}

print("📋 Workflow Definition:")
for key, value in workflow_definition.items():
    if key == "tasks":
        print(f"\n  {key}:")
        for i, task in enumerate(value, 1):
            print(f"    Task {i}: {task.get('task_key')}")
            if 'depends_on' in task:
                print(f"            Depends on: {', '.join(task['depends_on'])}")
    else:
        print(f"  {key}: {value}")

# COMMAND ----------

# DBTITLE 1,🔧 Creating a Workflow
# MAGIC %md
# MAGIC ### 🔧 How to Create Databricks Workflow:
# MAGIC
# MAGIC **Step 1: Via Databricks UI**
# MAGIC 1. Navigate to **Workflows** in left sidebar
# MAGIC 2. Click **Create Job**
# MAGIC 3. Add tasks (notebooks, Python scripts, SQL queries)
# MAGIC 4. Define task dependencies
# MAGIC 5. Configure cluster settings (use serverless for cost optimization)
# MAGIC 6. Set schedule (cron expression or manual)
# MAGIC 7. Add notifications
# MAGIC 8. Save and run
# MAGIC
# MAGIC **Step 2: Via Databricks CLI**
# MAGIC ```bash
# MAGIC databricks jobs create --json-file job_definition.json
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Via REST API**
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC api_url = "https://<workspace-url>/api/2.1/jobs/create"
# MAGIC headers = {"Authorization": f"Bearer {token}"}
# MAGIC response = requests.post(api_url, json=workflow_definition, headers=headers)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔗 Azure Data Factory Integration:
# MAGIC
# MAGIC **ADF Pipeline Activity:**
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "Run_Databricks_Notebook",
# MAGIC   "type": "DatabricksNotebook",
# MAGIC   "linkedServiceName": "DatabricksLinkedService",
# MAGIC   "typeProperties": {
# MAGIC     "notebookPath": "/Users/user@company.com/ETL_Pipeline",
# MAGIC     "baseParameters": {
# MAGIC       "catalog": "training_catalog",
# MAGIC       "schema": "ecommerce_schema"
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - Cross-platform orchestration
# MAGIC - Enterprise monitoring
# MAGIC - Cost tracking
# MAGIC - Compliance integration

# COMMAND ----------

# DBTITLE 1,📡 Section 9 - Monitoring & Reliability
# MAGIC %md
# MAGIC ## 📡 SECTION 9 — Monitoring & Reliability
# MAGIC
# MAGIC ### 🎯 Ensuring Pipeline Reliability:
# MAGIC
# MAGIC **ELI5:**  
# MAGIC Monitoring is like having a health tracker for your pipeline. It tells you:
# MAGIC - Is the pipeline running on time?
# MAGIC - Did anything fail?
# MAGIC - How much data was processed?
# MAGIC - How long did it take?
# MAGIC
# MAGIC **Architect View:**  
# MAGIC Production pipelines require **comprehensive observability**:
# MAGIC - **Health Monitoring**: Pipeline status, success/failure rates
# MAGIC - **Performance Monitoring**: Execution time, throughput
# MAGIC - **Data Quality Monitoring**: Row counts, schema validation
# MAGIC - **Cost Monitoring**: Compute usage, storage costs
# MAGIC - **Alerting**: Proactive notifications on anomalies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Key Reliability Patterns:
# MAGIC
# MAGIC #### 1. **Checkpointing** (Exactly-Once Processing)
# MAGIC ```python
# MAGIC .option("checkpointLocation", "/path/to/checkpoint")
# MAGIC ```
# MAGIC - Tracks processed data
# MAGIC - Enables restart from failure point
# MAGIC - Prevents duplicate processing
# MAGIC
# MAGIC #### 2. **Idempotency** (Safe Re-runs)
# MAGIC - Use `MERGE` instead of `INSERT`
# MAGIC - Upsert patterns with deduplication
# MAGIC - Deterministic transformations
# MAGIC
# MAGIC #### 3. **Schema Evolution**
# MAGIC ```python
# MAGIC .option("mergeSchema", "true")
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC ```
# MAGIC
# MAGIC #### 4. **Data Quality Expectations**
# MAGIC - Null checks
# MAGIC - Range validations
# MAGIC - Referential integrity
# MAGIC - Custom business rules
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Monitoring Metrics:

# COMMAND ----------

# DBTITLE 1,📊 Data Quality Checks
# ===================================================================
# DATA QUALITY: Implement quality checks on Gold layer
# ===================================================================

from pyspark.sql.functions import *

# Read Gold table for validation
df_gold_check = spark.read.format("delta").table(GOLD_TABLE_CATEGORY)

# Quality Check 1: Row count validation
row_count = df_gold_check.count()
print(f"\u2705 Quality Check 1: Row Count")
print(f"   Total rows: {row_count}")
assert row_count > 0, "FAIL: No data in Gold table!"

# Quality Check 2: Null validation
null_checks = df_gold_check.select(
    [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
     for c in ["category", "total_orders", "total_revenue"]]
).collect()[0]

print(f"\n✅ Quality Check 2: Null Validation")
for field, null_count in null_checks.asDict().items():
    print(f"   {field}: {null_count} nulls")
    assert null_count == 0, f"FAIL: Nulls found in {field}"

# Quality Check 3: Business logic validation
negative_revenue = df_gold_check.filter(col("total_revenue") < 0).count()
print(f"\n✅ Quality Check 3: Business Logic")
print(f"   Negative revenue records: {negative_revenue}")
assert negative_revenue == 0, "FAIL: Negative revenue detected!"

# Quality Check 4: Freshness check
max_date = df_gold_check.agg(max("order_date")).collect()[0][0]
print(f"\n✅ Quality Check 4: Data Freshness")
print(f"   Latest order date: {max_date}")

print("\n✅ All Quality Checks Passed!")

# COMMAND ----------

# DBTITLE 1,📊 Pipeline Performance Metrics
# ===================================================================
# MONITORING: Capture pipeline performance metrics
# ===================================================================

import time
from datetime import datetime

# Create monitoring metrics DataFrame
metrics_data = [
    {
        "pipeline_name": "ecommerce_etl",
        "layer": "Bronze",
        "table_name": BRONZE_TABLE,
        "record_count": spark.read.format("delta").table(BRONZE_TABLE).count(),
        "execution_timestamp": datetime.now(),
        "status": "SUCCESS"
    },
    {
        "pipeline_name": "ecommerce_etl",
        "layer": "Silver",
        "table_name": SILVER_TABLE,
        "record_count": spark.read.format("delta").table(SILVER_TABLE).count(),
        "execution_timestamp": datetime.now(),
        "status": "SUCCESS"
    },
    {
        "pipeline_name": "ecommerce_etl",
        "layer": "Gold",
        "table_name": GOLD_TABLE_CATEGORY,
        "record_count": spark.read.format("delta").table(GOLD_TABLE_CATEGORY).count(),
        "execution_timestamp": datetime.now(),
        "status": "SUCCESS"
    }
]

df_metrics = spark.createDataFrame(metrics_data)

print("📊 Pipeline Execution Metrics:")
display(df_metrics)

# Optionally: Write metrics to a monitoring table
# df_metrics.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.pipeline_metrics")

# COMMAND ----------

# DBTITLE 1,🛠️ Failure Handling Strategies
# MAGIC %md
# MAGIC ### 🛠️ Failure Handling Strategies:
# MAGIC
# MAGIC #### 1. **Automatic Retries**
# MAGIC ```python
# MAGIC # In workflow configuration
# MAGIC max_retries = 3
# MAGIC retry_delay_seconds = 300  # 5 minutes
# MAGIC ```
# MAGIC
# MAGIC #### 2. **Dead Letter Queue**
# MAGIC ```python
# MAGIC # Write failed records to quarantine table
# MAGIC df_failed.write.format("delta").mode("append") \
# MAGIC     .saveAsTable(f"{CATALOG}.{SCHEMA}.failed_records")
# MAGIC ```
# MAGIC
# MAGIC #### 3. **Circuit Breaker**
# MAGIC ```python
# MAGIC # Stop processing if error rate exceeds threshold
# MAGIC error_rate = failed_count / total_count
# MAGIC if error_rate > 0.05:  # 5% threshold
# MAGIC     raise Exception("Error rate exceeded threshold")
# MAGIC ```
# MAGIC
# MAGIC #### 4. **Graceful Degradation**
# MAGIC ```python
# MAGIC # Continue processing valid records, log errors
# MAGIC try:
# MAGIC     process_record(record)
# MAGIC except Exception as e:
# MAGIC     log_error(record, e)
# MAGIC     continue  # Don't fail entire batch
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📧 Alerting Configuration:
# MAGIC
# MAGIC **Email Notifications:**
# MAGIC - Job failure alerts
# MAGIC - Data quality failures
# MAGIC - SLA violations (late runs)
# MAGIC - Anomaly detection (unusual row counts)
# MAGIC
# MAGIC **Integration Options:**
# MAGIC - PagerDuty
# MAGIC - Slack webhooks
# MAGIC - Microsoft Teams
# MAGIC - Custom HTTP endpoints

# COMMAND ----------

# DBTITLE 1,⚙️ Section 10 - End-to-End Execution
# MAGIC %md
# MAGIC ## ⚙️ SECTION 10 — End-to-End Execution Demo
# MAGIC
# MAGIC ### 🎯 Complete Pipeline Flow:
# MAGIC
# MAGIC ```
# MAGIC 📊 DATA FLOW:
# MAGIC
# MAGIC 1. Source Files (JSON)  
# MAGIC    ↓
# MAGIC 2. Auto Loader (cloudFiles)  
# MAGIC    ↓
# MAGIC 3. Bronze Layer (raw + metadata)  
# MAGIC    ↓
# MAGIC 4. Silver Layer (cleaned + validated)  
# MAGIC    ↓
# MAGIC 5. Gold Layer (aggregated KPIs)  
# MAGIC    ↓
# MAGIC 6. BI Dashboards / ML Models  
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💻 Complete Pipeline Execution:

# COMMAND ----------

# DBTITLE 1,🚀 End-to-End Pipeline Execution
# ===================================================================
# END-TO-END EXECUTION: Run complete pipeline
# This demonstrates the full medallion architecture flow
# ===================================================================

print("="*80)
print("🚀 STARTING END-TO-END PIPELINE EXECUTION")
print("="*80)

try:
    # Step 1: Verify Bronze Layer
    print("\n🔹 STEP 1: Verify Bronze Layer")
    bronze_count = spark.read.format("delta").table(BRONZE_TABLE).count()
    print(f"   ✅ Bronze records: {bronze_count}")
    
    # Step 2: Verify Silver Layer
    print("\n🔹 STEP 2: Verify Silver Layer")
    silver_count = spark.read.format("delta").table(SILVER_TABLE).count()
    print(f"   ✅ Silver records: {silver_count}")
    
    # Step 3: Verify Gold Layer - Category Sales
    print("\n🔹 STEP 3: Verify Gold Layer (Category Sales)")
    gold_category_count = spark.read.format("delta").table(GOLD_TABLE_CATEGORY).count()
    print(f"   ✅ Gold category aggregations: {gold_category_count}")
    
    # Step 4: Verify Gold Layer - Customer Summary
    print("\n🔹 STEP 4: Verify Gold Layer (Customer Summary)")
    gold_customer_count = spark.read.format("delta").table(GOLD_TABLE_CUSTOMER).count()
    print(f"   ✅ Gold customer records: {gold_customer_count}")
    
    # Step 5: Data Quality Summary
    print("\n🔹 STEP 5: Data Quality Summary")
    print(f"   Bronze → Silver retention: {(silver_count/bronze_count)*100:.1f}%")
    print(f"   Data quality score: PASSED")
    
    # Step 6: Performance Summary
    print("\n🔹 STEP 6: Performance Summary")
    print(f"   Total records processed: {bronze_count}")
    print(f"   Layers created: Bronze, Silver, Gold (2 tables)")
    print(f"   Governance: Unity Catalog enabled")
    print(f"   Format: Delta Lake with ACID")
    
    print("\n" + "="*80)
    print("🎉 END-TO-END PIPELINE EXECUTION SUCCESSFUL!")
    print("="*80)
    
except Exception as e:
    print(f"\n❌ ERROR: Pipeline execution failed")
    print(f"   Error: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,📊 Final Analytics Query - Business Insights
# MAGIC %sql
# MAGIC -- ===================================================================
# MAGIC -- FINAL ANALYTICS: Business insights from Gold layer
# MAGIC -- ===================================================================
# MAGIC
# MAGIC -- Query 1: Top performing categories
# MAGIC WITH category_performance AS (
# MAGIC   SELECT 
# MAGIC     category,
# MAGIC     SUM(total_orders) as total_orders,
# MAGIC     SUM(total_revenue) as total_revenue,
# MAGIC     AVG(avg_order_value) as avg_order_value,
# MAGIC     SUM(unique_customers) as total_customers
# MAGIC   FROM training_catalog.ecommerce_schema.gold_sales_by_category
# MAGIC   GROUP BY category
# MAGIC )
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   total_orders,
# MAGIC   ROUND(total_revenue, 2) as total_revenue,
# MAGIC   ROUND(avg_order_value, 2) as avg_order_value,
# MAGIC   total_customers,
# MAGIC   ROUND((total_revenue / SUM(total_revenue) OVER ()) * 100, 2) as revenue_share_pct
# MAGIC FROM category_performance
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,🏛️ Section 11 - Production Architecture
# MAGIC %md
# MAGIC ## 🏛️ SECTION 11 — Production Architecture Design
# MAGIC
# MAGIC ### 🎯 Complete Production Architecture:
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │                    DATA SOURCES                                      │
# MAGIC │  S3 / ADLS / GCS | Kafka | Event Hub | REST APIs | Databases    │
# MAGIC └────────────────────────┬────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ↓
# MAGIC ┌────────────────────────┴────────────────────────────────────┐
# MAGIC │                   INGESTION LAYER                                  │
# MAGIC │    Auto Loader | Spark Streaming | Kafka Connect | JDBC          │
# MAGIC │              Schema Inference & Evolution                         │
# MAGIC └────────────────────────┬────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ↓
# MAGIC ┌────────────────────────┴────────────────────────────────────┐
# MAGIC │                 STORAGE LAYER (Delta Lake)                       │
# MAGIC │                                                                  │
# MAGIC │  🥉 BRONZE: Raw data + metadata                               │
# MAGIC │     - Immutable landing zone                                     │
# MAGIC │     - Full history retained                                      │
# MAGIC │     - Partition by ingestion date                                │
# MAGIC │                                                                  │
# MAGIC │  🧼 SILVER: Cleaned & validated                               │
# MAGIC │     - Business rules applied                                     │
# MAGIC │     - Deduplicated                                               │
# MAGIC │     - Conformed dimensions                                       │
# MAGIC │                                                                  │
# MAGIC │  🥇 GOLD: Business aggregates                                  │
# MAGIC │     - Star schema / denormalized                                 │
# MAGIC │     - KPI tables                                                 │
# MAGIC │     - ML feature stores                                          │
# MAGIC └────────────────────────┬────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ↓
# MAGIC ┌────────────────────────┴────────────────────────────────────┐
# MAGIC │              GOVERNANCE LAYER (Unity Catalog)                    │
# MAGIC │  - Fine-grained access control (table/column/row)                │
# MAGIC │  - Data lineage tracking                                         │
# MAGIC │  - Audit logs                                                    │
# MAGIC │  - PII tagging & masking                                         │
# MAGIC │  - Delta Sharing                                                 │
# MAGIC └────────────────────────┬────────────────────────────────────┘
# MAGIC                          │
# MAGIC         ┌────────────────┴────────────────┐
# MAGIC         │                                 │
# MAGIC         ↓                                 ↓
# MAGIC ┌────────────────┐             ┌────────────────┐
# MAGIC │ ORCHESTRATION  │             │  CONSUMPTION   │
# MAGIC │  - Workflows   │             │  - Dashboards  │
# MAGIC │  - ADF         │             │  - ML Models   │
# MAGIC │  - Airflow     │             │  - APIs        │
# MAGIC │  - Event-based │             │  - Reports     │
# MAGIC └────────────────┘             └────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔑 Production Design Principles:
# MAGIC
# MAGIC #### 1. **Scalability**
# MAGIC - Serverless compute for elastic scaling
# MAGIC - Delta Lake for efficient storage
# MAGIC - Incremental processing patterns
# MAGIC
# MAGIC #### 2. **Reliability**
# MAGIC - Exactly-once processing (checkpointing)
# MAGIC - Automatic retries and error handling
# MAGIC - Dead letter queues for failed records
# MAGIC
# MAGIC #### 3. **Performance**
# MAGIC - Z-ordering / liquid clustering
# MAGIC - Partition pruning
# MAGIC - Predicate pushdown
# MAGIC - Optimized file sizes (1GB target)
# MAGIC
# MAGIC #### 4. **Cost Optimization**
# MAGIC - Serverless for variable workloads
# MAGIC - Table optimization (VACUUM, OPTIMIZE)
# MAGIC - Data lifecycle management (archival)
# MAGIC - Spot instances for non-critical workloads
# MAGIC
# MAGIC #### 5. **Security & Compliance**
# MAGIC - Unity Catalog access controls
# MAGIC - Encryption at rest and in transit
# MAGIC - PII detection and masking
# MAGIC - Audit logging
# MAGIC
# MAGIC #### 6. **Observability**
# MAGIC - Pipeline monitoring dashboards
# MAGIC - Data quality metrics
# MAGIC - Performance tracking
# MAGIC - Cost attribution

# COMMAND ----------

# DBTITLE 1,🤖 Using Genie Code Agent for This Project
# MAGIC %md
# MAGIC ## 🤖 Using Genie Code Agent for This Project
# MAGIC
# MAGIC ### 🎯 What is Genie Code Agent?
# MAGIC
# MAGIC Genie Code is Databricks' AI assistant that helps you:
# MAGIC - **Build pipelines** with natural language
# MAGIC - **Debug issues** automatically
# MAGIC - **Optimize queries** for performance
# MAGIC - **Generate code** following best practices
# MAGIC - **Explain concepts** in ELI5 or technical terms
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💬 Example Prompts for This Project:
# MAGIC
# MAGIC #### **Ingestion & Bronze Layer:**
# MAGIC ```
# MAGIC 💬 "Create an Auto Loader pipeline to ingest JSON files from 
# MAGIC      /Volumes/training_catalog/ecommerce_schema/project_volume/raw 
# MAGIC      into a Bronze Delta table with metadata tracking"
# MAGIC
# MAGIC 💬 "Add schema evolution support to my Auto Loader stream"
# MAGIC
# MAGIC 💬 "Show me how to track source file paths in my Bronze layer"
# MAGIC ```
# MAGIC
# MAGIC #### **Silver Layer Processing:**
# MAGIC ```
# MAGIC 💬 "Clean and validate the Bronze orders table - remove nulls, 
# MAGIC      deduplicate by order_id, and add a total_amount column"
# MAGIC
# MAGIC 💬 "Create a Silver layer that filters out invalid price values
# MAGIC      and standardizes the category names"
# MAGIC
# MAGIC 💬 "Help me implement deduplication logic in my streaming pipeline"
# MAGIC ```
# MAGIC
# MAGIC #### **Gold Layer Aggregations:**
# MAGIC ```
# MAGIC 💬 "Aggregate Silver orders by category and date to calculate
# MAGIC      total revenue, order count, and average order value"
# MAGIC
# MAGIC 💬 "Create a customer segmentation table based on lifetime value"
# MAGIC
# MAGIC 💬 "Build a Gold table that shows daily sales trends by category"
# MAGIC ```
# MAGIC
# MAGIC #### **Governance:**
# MAGIC ```
# MAGIC 💬 "Add comments and tags to my Unity Catalog tables"
# MAGIC
# MAGIC 💬 "Show me how to view data lineage for my Silver table"
# MAGIC
# MAGIC 💬 "Help me set up column-level access control"
# MAGIC ```
# MAGIC
# MAGIC #### **Orchestration:**
# MAGIC ```
# MAGIC 💬 "Create a workflow that runs Bronze → Silver → Gold in sequence"
# MAGIC
# MAGIC 💬 "Schedule this notebook to run daily at 8 AM"
# MAGIC
# MAGIC 💬 "Add error handling and retries to my pipeline"
# MAGIC ```
# MAGIC
# MAGIC #### **Debugging & Optimization:**
# MAGIC ```
# MAGIC 💬 "Why is my streaming query failing with schema mismatch?"
# MAGIC
# MAGIC 💬 "Optimize my Gold table for faster dashboard queries"
# MAGIC
# MAGIC 💬 "Explain this error: [paste error message]"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Best Practices for Using Genie:
# MAGIC
# MAGIC 1. **Be Specific**: Include table names, paths, and requirements
# MAGIC 2. **Provide Context**: Mention the layer (Bronze/Silver/Gold) and goal
# MAGIC 3. **Ask Follow-ups**: Genie maintains conversation context
# MAGIC 4. **Request Explanations**: Ask "Explain like I'm 5" or "Architect view"
# MAGIC 5. **Iterate**: Refine the code through conversation

# COMMAND ----------

# DBTITLE 1,🎓 Final Summary & Key Learnings
# MAGIC %md
# MAGIC ## 🎓 FINAL SUMMARY & KEY LEARNINGS
# MAGIC
# MAGIC ### 🏆 What You Built:
# MAGIC
# MAGIC A **production-grade end-to-end data engineering pipeline** featuring:
# MAGIC
# MAGIC ✅ **Ingestion**: Auto Loader with schema inference and evolution  
# MAGIC ✅ **Storage**: Delta Lake with Medallion architecture (Bronze/Silver/Gold)  
# MAGIC ✅ **Processing**: PySpark Structured Streaming for real-time data  
# MAGIC ✅ **Governance**: Unity Catalog for metadata, lineage, and access control  
# MAGIC ✅ **Orchestration**: Workflows for automated pipeline execution  
# MAGIC ✅ **Monitoring**: Data quality checks and performance metrics  
# MAGIC ✅ **Best Practices**: Checkpointing, idempotency, schema evolution  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Key Concepts Mastered:
# MAGIC
# MAGIC 1. **Medallion Architecture**
# MAGIC    - Bronze: Raw data landing zone
# MAGIC    - Silver: Cleaned and validated data
# MAGIC    - Gold: Business-ready aggregates
# MAGIC
# MAGIC 2. **Auto Loader**
# MAGIC    - Efficient file ingestion from cloud storage
# MAGIC    - Schema inference and evolution
# MAGIC    - Exactly-once processing guarantees
# MAGIC
# MAGIC 3. **Delta Lake**
# MAGIC    - ACID transactions
# MAGIC    - Time travel
# MAGIC    - Schema evolution
# MAGIC    - Optimizations (Z-ordering, OPTIMIZE, VACUUM)
# MAGIC
# MAGIC 4. **Unity Catalog**
# MAGIC    - Three-level namespace (Catalog.Schema.Table)
# MAGIC    - Fine-grained access control
# MAGIC    - Data lineage tracking
# MAGIC    - Metadata management
# MAGIC
# MAGIC 5. **Streaming vs Batch**
# MAGIC    - When to use streaming (continuous ingestion)
# MAGIC    - When to use batch (periodic aggregations)
# MAGIC    - Hybrid approaches
# MAGIC
# MAGIC 6. **Data Quality**
# MAGIC    - Validation rules
# MAGIC    - Null handling
# MAGIC    - Deduplication
# MAGIC    - Business logic enforcement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 Common Mistakes to Avoid:
# MAGIC
# MAGIC #### 1. **Mixing Medallion Layers**
# MAGIC ❌ **Wrong**: Applying business logic in Bronze layer  
# MAGIC ✅ **Right**: Bronze = raw data only, apply transformations in Silver
# MAGIC
# MAGIC #### 2. **Missing Checkpoints**
# MAGIC ❌ **Wrong**: No checkpoint location for streaming queries  
# MAGIC ✅ **Right**: Always specify `.option("checkpointLocation", "<path>")`
# MAGIC
# MAGIC #### 3. **Ignoring Schema Evolution**
# MAGIC ❌ **Wrong**: Pipeline breaks when new columns arrive  
# MAGIC ✅ **Right**: Enable schema evolution with `mergeSchema` and `schemaEvolutionMode`
# MAGIC
# MAGIC #### 4. **Using Local Storage**
# MAGIC ❌ **Wrong**: Writing to `/tmp` or local paths  
# MAGIC ✅ **Right**: Use Unity Catalog volumes or cloud storage
# MAGIC
# MAGIC #### 5. **No Governance**
# MAGIC ❌ **Wrong**: Tables created without catalog/schema, no metadata  
# MAGIC ✅ **Right**: Use Unity Catalog three-level namespace, add comments and tags
# MAGIC
# MAGIC #### 6. **Manual Orchestration**
# MAGIC ❌ **Wrong**: Running notebooks manually in sequence  
# MAGIC ✅ **Right**: Create workflows with task dependencies
# MAGIC
# MAGIC #### 7. **No Monitoring**
# MAGIC ❌ **Wrong**: Pipeline fails silently, no alerts  
# MAGIC ✅ **Right**: Implement data quality checks, logging, and notifications
# MAGIC
# MAGIC #### 8. **Inefficient Partitioning**
# MAGIC ❌ **Wrong**: No partitioning or over-partitioning (too many small files)  
# MAGIC ✅ **Right**: Partition by date/category with ~1GB files per partition
# MAGIC
# MAGIC #### 9. **Using RDDs**
# MAGIC ❌ **Wrong**: Using low-level RDD API  
# MAGIC ✅ **Right**: Use DataFrame/Dataset API for optimizations
# MAGIC
# MAGIC #### 10. **Cache/Persist in Serverless**
# MAGIC ❌ **Wrong**: Using `.cache()` or `.persist()`  
# MAGIC ✅ **Right**: Let Databricks manage caching automatically
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💼 Real-World Applications:
# MAGIC
# MAGIC * **E-commerce**: Real-time order processing and analytics
# MAGIC * **IoT**: Sensor data ingestion and monitoring
# MAGIC * **Financial Services**: Transaction processing and fraud detection
# MAGIC * **Healthcare**: Patient data aggregation and reporting
# MAGIC * **Retail**: Inventory management and demand forecasting
# MAGIC * **Marketing**: Campaign performance and customer segmentation

# COMMAND ----------

# DBTITLE 1,❓ Interview Questions - Phase 12 Day 44
# MAGIC %md
# MAGIC ## ❓ INTERVIEW QUESTIONS - End-to-End Data Engineering
# MAGIC
# MAGIC ### 🔴 **Question 1**: Explain the Medallion Architecture and why it's beneficial.
# MAGIC
# MAGIC **Answer**: The Medallion Architecture organizes data into three layers:
# MAGIC - **Bronze** (Raw): Immutable landing zone preserving source data exactly as received, enabling replay
# MAGIC - **Silver** (Curated): Cleaned, validated, deduplicated data conforming to business rules
# MAGIC - **Gold** (Aggregated): Business-ready KPIs and aggregates optimized for analytics
# MAGIC
# MAGIC **Benefits**: Separation of concerns, incremental processing, data quality progression, multi-speed processing, and simplified debugging by isolating transformations per layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 2**: How does Auto Loader differ from traditional file ingestion?
# MAGIC
# MAGIC **Answer**: 
# MAGIC **Traditional**: Manual directory listing, full scan on each run, no schema inference, prone to duplicates
# MAGIC
# MAGIC **Auto Loader**: 
# MAGIC - Efficient incremental processing using file notifications (preferred) or directory listing
# MAGIC - Automatic schema inference with evolution support
# MAGIC - Exactly-once processing via checkpointing
# MAGIC - Handles millions of files efficiently
# MAGIC - RocksDB-based state management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 3**: What is checkpointing in Spark Structured Streaming and why is it critical?
# MAGIC
# MAGIC **Answer**: Checkpointing stores the streaming query's progress (offsets, state) to cloud storage. It enables:
# MAGIC - **Exactly-once processing**: No duplicates or data loss
# MAGIC - **Fault tolerance**: Resume from last successful batch after failure
# MAGIC - **State recovery**: Maintain aggregation state across restarts
# MAGIC - **Idempotency**: Safe to re-run without side effects
# MAGIC
# MAGIC Without checkpointing, streaming queries process all data from the beginning on restart.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 4**: Explain the Unity Catalog hierarchy and its benefits.
# MAGIC
# MAGIC **Answer**: 
# MAGIC **Hierarchy**: `Metastore > Catalog > Schema > Tables/Views/Volumes`
# MAGIC
# MAGIC **Benefits**:
# MAGIC - **Centralized governance**: Single source of truth for metadata
# MAGIC - **Fine-grained access control**: Table, column, row-level permissions
# MAGIC - **Data lineage**: Track upstream/downstream dependencies
# MAGIC - **Cross-workspace**: Consistent namespace across workspaces
# MAGIC - **Delta Sharing**: Secure data sharing without copies
# MAGIC - **Audit logging**: Compliance and security tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 5**: When would you use `outputMode("append")` vs `outputMode("complete")`?
# MAGIC
# MAGIC **Answer**:
# MAGIC
# MAGIC **Append Mode**:
# MAGIC - Use for: Row-level transformations, filtering, enrichment
# MAGIC - Bronze and Silver layers
# MAGIC - Only new rows added to result table
# MAGIC - Efficient for large datasets
# MAGIC
# MAGIC **Complete Mode**:
# MAGIC - Use for: Aggregations without watermarks
# MAGIC - Gold layer aggregations
# MAGIC - Entire result table rewritten each trigger
# MAGIC - Higher memory/compute cost
# MAGIC
# MAGIC **Update Mode**: Best for aggregations with watermarks (only changed rows updated)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 6**: How do you handle schema evolution in production pipelines?
# MAGIC
# MAGIC **Answer**:
# MAGIC
# MAGIC **For Auto Loader**:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Allow new columns
# MAGIC .option("cloudFiles.schemaEvolutionMode", "rescue")  # Capture unexpected data
# MAGIC ```
# MAGIC
# MAGIC **For Delta writes**:
# MAGIC ```python
# MAGIC .option("mergeSchema", "true")  # Merge new columns into table
# MAGIC ```
# MAGIC
# MAGIC **Best practices**:
# MAGIC - Use schema hints for critical columns
# MAGIC - Monitor schema changes via alerts
# MAGIC - Version schemas in Git
# MAGIC - Test changes in dev/staging first
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 7**: Explain deduplication strategies in streaming pipelines.
# MAGIC
# MAGIC **Answer**:
# MAGIC
# MAGIC **Strategy 1: Window-based deduplication** (used in this notebook)
# MAGIC ```python
# MAGIC window_spec = Window.partitionBy("order_id").orderBy(col("timestamp").desc())
# MAGIC df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1)
# MAGIC ```
# MAGIC
# MAGIC **Strategy 2: dropDuplicates with watermark**
# MAGIC ```python
# MAGIC df.withWatermark("timestamp", "1 hour").dropDuplicates(["order_id"])
# MAGIC ```
# MAGIC
# MAGIC **Strategy 3: MERGE (upsert pattern)**
# MAGIC ```python
# MAGIC DeltaTable.forPath(spark, target_path).alias("target") \
# MAGIC     .merge(source.alias("source"), "target.id = source.id") \
# MAGIC     .whenMatchedUpdateAll() \
# MAGIC     .whenNotMatchedInsertAll() \
# MAGIC     .execute()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 8**: How would you optimize a slow Gold layer aggregation query?
# MAGIC
# MAGIC **Answer**:
# MAGIC
# MAGIC 1. **Z-ordering**: 
# MAGIC ```sql
# MAGIC OPTIMIZE gold_table ZORDER BY (category, order_date)
# MAGIC ```
# MAGIC
# MAGIC 2. **Liquid Clustering** (newer approach):
# MAGIC ```sql
# MAGIC ALTER TABLE gold_table CLUSTER BY (category, order_date)
# MAGIC ```
# MAGIC
# MAGIC 3. **Partitioning**: Partition by frequently filtered column (e.g., date)
# MAGIC
# MAGIC 4. **Incremental aggregation**: Process only new data
# MAGIC ```python
# MAGIC df_new = spark.read.format("delta").option("readChangeFeed", "true") \
# MAGIC     .option("startingVersion", last_version).table(silver_table)
# MAGIC ```
# MAGIC
# MAGIC 5. **Caching/Materialized views**: For frequently accessed aggregates
# MAGIC
# MAGIC 6. **File compaction**: Keep files ~1GB
# MAGIC ```sql
# MAGIC OPTIMIZE gold_table
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 9**: Design a disaster recovery strategy for this pipeline.
# MAGIC
# MAGIC **Answer**:
# MAGIC
# MAGIC **1. Data backup**:
# MAGIC - Delta Lake time travel: Restore to any previous version
# MAGIC - Replicate Delta tables across regions (Delta Sharing, `COPY INTO`)
# MAGIC - Retain Bronze layer indefinitely for replay
# MAGIC
# MAGIC **2. Checkpoint backup**:
# MAGIC - Copy checkpoints to secondary region
# MAGIC - Version checkpoints with pipeline deployments
# MAGIC
# MAGIC **3. Metadata backup**:
# MAGIC - Unity Catalog syncs across regions
# MAGIC - Export table definitions to Git
# MAGIC
# MAGIC **4. Recovery steps**:
# MAGIC - Restore Bronze data from backup
# MAGIC - Re-run pipeline from Bronze with fresh checkpoints
# MAGIC - Validate data quality post-recovery
# MAGIC
# MAGIC **5. Testing**: Regularly test DR procedures (quarterly)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔴 **Question 10**: How would you integrate this pipeline with Azure Data Factory?
# MAGIC
# MAGIC **Answer**:
# MAGIC
# MAGIC **Step 1: Create ADF Linked Service**
# MAGIC - Configure Databricks workspace connection
# MAGIC - Use service principal or managed identity for auth
# MAGIC
# MAGIC **Step 2: Create ADF Pipeline**
# MAGIC ```json
# MAGIC {
# MAGIC   "activities": [
# MAGIC     {
# MAGIC       "name": "RunBronzeIngestion",
# MAGIC       "type": "DatabricksNotebook",
# MAGIC       "linkedServiceName": "DatabricksLinkedService",
# MAGIC       "typeProperties": {
# MAGIC         "notebookPath": "/Path/To/This/Notebook",
# MAGIC         "baseParameters": {"layer": "bronze"}
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "name": "RunSilverProcessing",
# MAGIC       "dependsOn": [{"activity": "RunBronzeIngestion", "dependencyConditions": ["Succeeded"]}],
# MAGIC       "type": "DatabricksNotebook",
# MAGIC       ...
# MAGIC     }
# MAGIC   ],
# MAGIC   "triggers": [{"type": "ScheduleTrigger", "recurrence": {"frequency": "Hour", "interval": 1}}]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Monitoring**
# MAGIC - ADF pipeline monitoring dashboard
# MAGIC - Integrate with Azure Monitor
# MAGIC - Set up alerts for failures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🟽 Bonus: Cost Optimization Question
# MAGIC
# MAGIC **Question**: How would you optimize costs for this pipeline?
# MAGIC
# MAGIC **Answer**:
# MAGIC 1. **Use Serverless**: Auto-scaling, pay-per-use
# MAGIC 2. **Photon Engine**: 2-3x faster, lower cost
# MAGIC 3. **Optimize trigger frequency**: Batch smaller loads
# MAGIC 4. **Table optimization**: `OPTIMIZE`, `VACUUM` to reduce storage
# MAGIC 5. **Spot instances**: For non-critical batch workloads
# MAGIC 6. **Lifecycle policies**: Archive old Bronze data to cheaper storage
# MAGIC 7. **Monitor costs**: Use Databricks System Tables for cost attribution
# MAGIC 8. **Right-size clusters**: Don't over-provision compute

# COMMAND ----------

# DBTITLE 1,🏁 Conclusion
# MAGIC %md
# MAGIC ## 🏁 CONCLUSION
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎉 Congratulations!
# MAGIC
# MAGIC You've successfully completed **Phase 12 Day 44** and built a comprehensive **end-to-end data engineering pipeline** using:
# MAGIC
# MAGIC ✅ **Auto Loader** for efficient ingestion  
# MAGIC ✅ **Delta Lake** for reliable storage  
# MAGIC ✅ **Medallion Architecture** for data quality progression  
# MAGIC ✅ **Unity Catalog** for governance  
# MAGIC ✅ **Structured Streaming** for real-time processing  
# MAGIC ✅ **Orchestration** patterns for automation  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Next Steps:
# MAGIC
# MAGIC 1. **Execute this notebook** on Databricks serverless compute
# MAGIC 2. **Modify the sample data** to match your use case
# MAGIC 3. **Create a Workflow** to automate the pipeline
# MAGIC 4. **Build a BI dashboard** on top of Gold tables
# MAGIC 5. **Implement advanced features**:
# MAGIC    - Change Data Capture (CDC)
# MAGIC    - Slowly Changing Dimensions (SCD)
# MAGIC    - Data quality expectations
# MAGIC    - ML model integration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📚 Additional Resources:
# MAGIC
# MAGIC * **Databricks Documentation**: [docs.databricks.com](https://docs.databricks.com)
# MAGIC * **Delta Lake Guide**: [delta.io](https://delta.io)
# MAGIC * **Auto Loader**: [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/)
# MAGIC * **Unity Catalog**: [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/)
# MAGIC * **Databricks Community**: [community.databricks.com](https://community.databricks.com)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 👏 Thank You!
# MAGIC
# MAGIC **Author**: TRRaveendra  
# MAGIC **Watermark**: @TRRaveendra  
# MAGIC **Phase**: 12 | **Day**: 44  
# MAGIC **Topic**: End-to-End Data Engineering Project  
# MAGIC
# MAGIC **Remember**: Practice makes perfect. Build, break, learn, iterate!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🚀 **Happy Data Engineering!** 🚀