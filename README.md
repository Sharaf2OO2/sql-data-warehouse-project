# SQL Data Warehouse Project

## Overview

This project implements a modern data warehouse using the Medallion Architecture pattern, comprising Bronze, Silver, and Gold layers. It processes data from CRM and ERP systems to provide a scalable and analytics-ready data model.

## Purpose and Scope

The SQL Data Warehouse Project is designed to:

1. Ingest raw data from multiple source systems (CRM and ERP).
2. Cleanse and standardize data through structured transformations.
3. Model data into an analytics-ready star schema.
4. Implement data quality checks to ensure data reliability.
5. Provide a scalable architecture for data analytics.

## Medallion Architecture

![Data Warehouse Arch](https://github.com/user-attachments/assets/3f020f87-b675-49d0-b09f-66009bb046ce)


### Layer Purposes

- **Bronze Layer**: Raw data ingestion from source systems without transformations.
- **Silver Layer**: Data cleansing, standardization, and validation.
- **Gold Layer**: Business-ready data modeling in a star schema format.

## Data Flow Process

1. **Data Ingestion**: Load source files from CRM and ERP systems into Bronze layer tables.
2. **Data Cleansing**: Transform and standardize data, loading into Silver layer tables.
3. **Data Validation**: Perform quality checks on Silver layer data.
4. **Data Modeling**: Transform Silver data into a star schema in the Gold layer.
5. **Model Validation**: Conduct quality checks on Gold layer data.

## Key Components

### Schema Definitions

Three database schemas are created to separate data by processing stage:

- `bronze` schema for raw data.
- `silver` schema for cleansed data.
- `gold` schema for analytics-ready data.

### Data Loading Processes

ETL procedures for loading data between layers:

- Load from source files to Bronze layer tables.
- Transform and load from Bronze to Silver layer tables.
- Model and load from Silver to Gold layer tables.

### Data Quality Framework

Quality checks are implemented at two levels:

- **Silver layer checks**: Validate data completeness, consistency, and format.
- **Gold layer checks**: Validate dimensional integrity and referential constraints.

### Source Systems Integration

Integration with two primary source systems:

- **CRM system**: Provides customer, product, and sales data.
- **ERP system**: Provides additional customer, location, and product category data.

## Benefits

- **Data Lineage**: Clear tracking of data from source to analytical model.
- **Data Quality**: Comprehensive quality checks at multiple levels.
- **Transformation Isolation**: Separate layers for different transformation stages.
- **Raw Data Preservation**: Original data preserved in the Bronze layer.
- **Optimized Analytics**: Star schema design for efficient querying.
- **Scalable Architecture**: Modular design that can accommodate growth.


## License

This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.

## Contact

For questions or collaboration opportunities, feel free to reach out:

- [**Gmail**](mailto:sharafahmed2002@gmail.com)
- [**LinkedIn**](https://www.linkedin.com/in/sharaf-ahmed-72955b248/)
- [**X**](https://x.com/SharafAhmed_)

---
For more information, visit the [DeepWiki project page](https://deepwiki.com/Sharaf2OO2/sql-data-warehouse-project)
