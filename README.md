# Production-Scale-Incremental-Data-Platform-with-DAB-and-CICD

## Architecture Overview
This repository contains the complete implementation of a production-grade, end-to-end data platform built with modern cloud-native DevOps principles. The core objective of this project is to showcase **environment-decoupled parameterization**—enabling a single, dynamic codebase to orchestrate and process data safely across isolated **Development (Dev)** and **Production (Prod)** targets without code duplication or hardcoded parameters.

## Demo Video
[![Project Demo](https://img.shields.io/badge/Watch-Demo%20Video-red?style=for-the-badge&logo=youtube)](https://youtu.be/NLJHYP9fohs?si=oWbnQMhmbw9StdAY)
*Developed by Rabab Abid — Production-Grade Data Engineering Showcase Portfolio.* 

### System Architecture Diagram
![Complete Project Architecture](media/Project%20Architecture.png)

### Core Technology Stack
1. **Source System:** Azure SQL Database (Transactional Metadata & CDC Logs)
2. **Orchestration & ETL:** Azure Data Factory (ADF)
3. **Data Lakehouse Storage:** Azure Data Lake Storage (ADLS Gen2 Layered Architecture: Bronze/Silver/Gold)
4. **Secret Management:** Azure Key Vault
5. **Identity & Governance:** Microsoft Entra ID (IAM) & Unity Catalog
6. **Data Compute Engine:** Azure Databricks (Spark SQL, PySpark, Autoloader, SCD Type 1 & 2 using LDP/SDP, Class import)
7. **Dynamic SQL:** Jinja2 Python Templating
8. **Infrastructure as Code (IaC):** Databricks Asset Bundles (DABs) using YAML declarative manifests
9. **Version Control & DevOps:** Git, GitHub Desktop, & GitHub Actions (CI/CD Automations)
10. **Alerting System:** Azure Logic Apps Engine (Webhooks and HTTP Listeners)

## Pipeline Architecture
### Bronze Layer — Incremental Ingestion via ADF

![ADF outer pipeline showing Lookup + ForEach](media/ADF-outer%20loop.png)

ADF incremental_loop pipeline reads a loop_input.json configuration file via Lookup activity to dynamically process all five source tables in a single pipeline run.

![Inside ForEach — full CDC & Backfilling logic](media/ADF-inside%20for%20loop.png)

Inside ForEach: per-table CDC watermark file is read, Copy activity extracts only new records from Azure SQL DB using the watermark date, IF condition checks dataRead value — on data present, Script activity fetches new max CDC date and Copy activity updates the watermark file for next run. On no data, empty file is deleted automatically.

Key features:
- CDC watermarking with per-table JSON files in ADLS
- Backfilling supported via from_date parameter in loop_input.json
- Dynamic SQL query built from ForEach item properties

### Silver Layer — Autoloader + PySpark Transformations
* **Custom Processing Classes:** Built custom Python utility classes that encapsulate repetitive cleaning rules, schema enforcement, and standard metadata column casting.
* **Optimized Streaming Ingestion:** Leverages **Apache Spark AutoLoader** inside Databricks Silver notebooks to read incoming data incrementally with fully managed schema evolution tracking, minimizing computing footprints.
* * **Custom Processing Classes:** Built custom Python utility classes that encapsulate repetitive cleaning rules, schema enforcement, and standard metadata column casting.

### Jinja Dynamic SQL — One Big Table
Jinja2 Python templating library used to dynamically generate SQL joins across fact and dimension tables. Template iterates over a parameters list to build SELECT, FROM, LEFT JOIN, and WHERE clauses — no hardcoded SQL strings.

### Gold Layer — SCD via Lakeflow Declarative Pipelines

![Databricks Jobs graph view](media/Databricks%20DEV%20job%20DAG.png)

![Gold pipeline DAG view](media/Databricks%20DEV%20gold%20pipeline%20DAG.png)

gold_model_autocdc.py implements:
- SCD Type 2 for Dim Tables — full history preserved, END_AT date populated on record change
- SCD Type 1 for FactStream — upsert only, no history
- Data quality expectations enforced via expect_all_or_drop

### Infrastructure as Code via Databricks Asset Bundles (DABs)
The entire Databricks deployment lives as code within declarative YAML manifests (`databricks.yml`, `job.yml`, `pipeline.yml`).
* This decouples developers from manual  setups in the Databricks UI.
* Variables like target database catalogs are fully parameterized utilizing native configuration maps, compiling seamlessly during deployment validation.

### CI/CD via GitHub Actions
Two workflow files implement full deployment automation:

deploy-dev.yml & deploy-prod.yml triggers on every push to dev/prod branch with changes in databricks/ folder:
1. Checkout repository
2. Install Databricks CLI
3. Validate bundle
4. Deploy to dev/prod environment

### ADF Integration with DAB Deployed Jobs
![ADF pipeline extended view with Databricks Job activity](media/ADF-complete%20view%20with%20WebAlert%20&%20Databricks%20Job.png)

ADF orchestration_pipeline is parameterized with Env parameter (dev or prod). IF condition routes to correct DAB-deployed Databricks job ID based on environment. Base parameters pass catalog_name to the Databricks job at trigger time — full environment isolation from a single ADF pipeline.

### LogicApps & WebActivity
![Logic Apps email configuration](media/logic%20app%20designer.png)

Web Activity connected on failure to all pipeline activities — HTTP POST to Logic Apps endpoint with pipeline name and run ID. Logic Apps sends email notification to me on any failure.

### Scheduled Trigger for production run
![ADF scheduled trigger for prod](media/Production%20ADF%20pipeline%20trigger.png)

Scheduled trigger configured for prod environment — pipeline runs automatically passing Env equals prod parameter. No manual trigger required for production runs.

## Secrets and Security

- Azure Key Vault stores Databricks access token
- ADF Databricks linked service references Key Vault secret — no credentials in pipeline configuration
- GitHub repository secrets store DATABRICKS_HOST and DATABRICKS_TOKEN for GitHub Actions
- Access Connector for Azure Databricks grants managed identity access to ADLS Gen2 — no storage account keys in code
- Microsoft Entra ID manages workspace-level authentication
