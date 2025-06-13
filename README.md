# Airflow ETL Pipeline with Email Notification

This project implements an ETL (Extract, Transform, Load) pipeline using **Apache Airflow**. The DAG extracts data from a given source, performs necessary transformations, loads it into a destination and sends an email notification upon completion.

## Features

- **Extract**: Reads transaction data from CSV
- **Transform**: Cleans and enriches the data (formats dates, handles missing values, calculates tax)
- **Load**: Inserts the cleaned data into a PostgreSQL database
- **Notify**: Sends an email upon successful completion of the pipeline

## Project Structure

<p align="center">
  <img src="https://github.com/HieuCorn364/Airflow-ETL/blob/main/images/Structure%20Project.png" width="600"/>
</p>

## Requirements

- Docker & Docker Compose
- Gmail account (or SMTP server credentials) for email notifications
- PostgreSQL database (included in Docker Compose)

**Data Description**
| Column Name     | Description                            |
|-----------------|----------------------------------------|
| `Transaction_ID`| Unique identifier for each transaction |
| `Customer_Name` | Name of the customer                   |
| `Date`          | Transaction date (YYYY-MM-DD)         |
| `Product`       | Product purchased                      |
| `Total_Amount`  | Transaction amount                     |
| `Ratings`       | Customer rating (optional)            |
| `Feedback`      | Customer feedback (optional)          |

### **Sample Data Preview**

![Data Sample](https://github.com/HieuCorn364/Airflow-ETL/blob/main/images/Data%20Sample.png)

###  **Transformations Applied**
-  Date format changed to `YYYY/MM/DD`
-  `Total_Amount` converted to float
-  Missing `Feedback` → `"No Feedback"`
-  Missing `Ratings` → `0`
-  New column `Tax = Total_Amount * 0.1`
  
##  Email Notification

- **SMTP Server**: `smtp.gmail.com` (TLS, port 587)
- **Subject**: `"ETL Pipeline Completed"`
- **Body**: `"The ETL pipeline for transaction data has completed successfully."`
- **Security**: Uses **Gmail App Password** for authentication.

###  **Chosen Approach: `smtplib (PythonOperator)`**

>  I use **`smtplib`** for its **flexibility** and **independence** from Airflow's providers.  
>  Suitable for projects that require **complex or dynamic email contents**.

## ETL Pipeline Execution Result

After running the DAG successfully in Airflow, all tasks were completed as expected.

### **Tasks Completion Screenshot**

![Tasks Completed](https://github.com/HieuCorn364/Airflow-ETL/blob/main/images/Tasks%20completed.png)

-  **extract** → Extracted data from CSV
-  **transform** → Cleaned and transformed the data
-  **load** → Loaded into PostgreSQL
-  **send_email** → Email notification sent successfully

