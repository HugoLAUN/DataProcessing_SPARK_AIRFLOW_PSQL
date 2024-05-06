# Data processing with Spark / Airflow / PSQL 

## Introduction

This project is aiming to convert a csv file into multiple tables in order to match a star schema.
During this project we will use the folowing tools: Spark / Airflow / PSQL

The original csv have this structure:  
'Car_id': String  
'Date': Date  
'Customer Name': String  
'Gender': String  
'Annual Income': integer 
'Dealer_Name': String 
'Company': String  
'Model': String  
'Engine': String  
'Transmission': String  
'Color': String  
'Price ($)': integer  
'Dealer_No ': String  
'Body Style': String  
'Phone': String  
'Dealer_Region': String  

We want to have this schema in psql at the end:
![image](/assets/MCD.png)

## PSQL set up

### Create database:
```
CREATE DATABASE car_sales;
```
### Create user:
```
CREATE USER usr WITH PASSWORD 'usr';
```
### Grant privileges :
```
GRANT ALL PRIVILEGES ON DATABASE car_sales TO usr;
```
### GRANT access to public schema:
```
\q
psql car_sales;
GRANT ALL ON SCHEMA public TO usr;
```

## Spark job
The Spark job consist in:  
- Loading the CSV file
- Create a dataframe for each table
   - Also create the id needed
- Connect to Psql
- Save into Psql


## Airflow DAG
The Airflow DAG consist in:  
- Setting up the default args
- Setting up the DAG settings
- Setting up the JOB settings

## Execution

### Start Airflow webserver
```
airflow webserver
```

### Start Airflow scheduler
```
airflow scheduler
```

### Launch the DAG in Airflow
![image](/assets/airflow.png)

### Tables are now in Psql
![image](/assets/database.png)

## Upgrades to consider

### Docker
Containerize the project in Docker for easier deployment.

### Spark ML
Using Spark ML to create models in order to make predictions, for exemple to predict which car a futur client will buy based on their salary and the dealer.
