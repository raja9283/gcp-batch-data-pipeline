# GCP Batch Data Pipeline Project

## Project Overview
This project implements a robust batch data pipeline on Google Cloud Platform (GCP) utilizing services such as Cloud Storage, Dataproc, BigQuery, and Cloud Composer (Airflow). Leveraging the capabilities of PySpark, the pipeline handles complex operations like joins, window functions, and data transformations to efficiently process sales data encompassing customer, product, orders, order_items, and transaction information. It extracts valuable insights, including order summaries and monthly sales, which are visualized in a Looker Studio dashboard.

## Architecture Overview

1. **GCP Storage**: Serves as the source for sales data.
2. **Cloud Dataproc**: Facilitates data transformation using PySpark, allowing for efficient processing and analysis of large datasets.
3. **BigQuery**: Acts as the destination for storing the transformed data, enabling quick querying and analysis.
4. **Cloud Composer**: Orchestrates the scheduling and execution of data pipeline jobs via Airflow.
5. **Looker Studio**: Provides data visualization for the insights derived from the transformed data.

## GCP Batch Data Pipeline Architecture
![architecture_diagram drawio (1)](https://github.com/user-attachments/assets/1a5efe1d-2b25-44cb-aa52-52e9b7508329)


## Prerequisites

- Google Cloud account with permissions to create and manage Cloud Storage, Dataproc, BigQuery, and Cloud Composer resources.
- Python 3.x installed on your local environment.
- Necessary Python packages listed in `requirements.txt`.

## GCP Setup

1. **Create a Google Cloud Storage Bucket**: Set up a storage bucket to hold your sales data.
2. **Set Up Dataproc Cluster**: Create a Dataproc cluster to run your PySpark jobs.
3. **Create BigQuery Dataset**: Prepare a dataset in BigQuery to store the transformed data.
4. **Deploy Cloud Composer Environment**: Set up a Cloud Composer environment to manage Airflow DAGs for orchestration.

## How to Run

1. Generate sample data using the scripts in the `data` folder.
2. Upload the sample data to the Google Cloud Storage bucket.
3. Trigger the Airflow DAGs to initiate the data transformation process.
4. Monitor the progress via Cloud Composer and visualize insights in Looker Studio.

## Airflow DAG Execution Flow
<img width="1252" alt="sales_analysis_airflow_dag" src="https://github.com/user-attachments/assets/acb9cfd3-f135-4475-a6ce-5e563d53c8c4">

## Looker Studio Dashboard Visualization
![Sales_Analysis_Looker_Dashboard](https://github.com/user-attachments/assets/af9815f1-41bd-4f7e-916f-8c2975c6a85a)


## Repository Contents

- **`airflow/`**: Contains Airflow DAG creation code for managing data pipeline jobs.
- **`data/`**: Python scripts for generating sample sales data.
- **`pyspark/`**: PySpark code used for data transformation.
- **`.github/workflows/`**: GitHub Actions workflow files for CI/CD, uploading PySpark and Airflow files to Google Cloud Storage.
- **`requirements.txt`**: Lists all required Python packages for the project.
- **`sample_config.json`**: Configuration file containing important parameters such as bucket paths, table names, and other settings required for the pipeline execution. This file is essential for configuring data source and destination details.


## Troubleshooting & Debugging

- Check the logs in Google Cloud Console for errors in Cloud Composer and Dataproc.
- Ensure all API credentials and configurations in `sample_config.json` are correct.
- Validate that the input data matches the expected structure for processing.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
