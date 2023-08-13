# Walmart-Data-End-to-End-Pipeline

The project aimed to create an end-to-end sales data pipeline and derive actionable insights through a BI tool, utilizing technologies like S3, Lambda, CloudWatch, Glue, Athena, Airflow, EMR, Docker, and SuperSet. The integrated pipeline transformed raw data into meaningful insights, enabling data-driven decision-making and facilitating business growth.

![image](https://github.com/umergh7/Walmart-Data-End-to-End-Pipeline/assets/117035545/2cea6f5a-3cc8-400f-bd9e-3d213e240522)

In this case, we are simulating the data in Snowflake to act as an OLTP database. 

## 1. Snowflake
The goal here is to set up a S3 stage within snowflake and send queried data to an S3 bucket using a stored procedure and CRON job.

## 2. Lambda
The lambda function will be invoked everyday at 3AM and check if the S3 bucket has up to date data. If so, it will then send the data to airflow and using XCOMs you are able to access the data in airflow/EC2.

## 3. Airflow
The dag is comprised of 4 steps. First it will retrieve the files from the S3 bucket with the help of lamdba as explained earlier, automatically create and disable a cluster, add steps to the cluster, and a step sensor to check once the dag has been completed.

## 4. EMR
The pyspark script will use various tables to create a fact table. The fact table is aiming to answer the following questions:

* total sales quantity of a product : Sum(sales_qty)
* total sales amount of a product : Sum(sales_amt)
* average sales Price: Sum(sales_amt)/Sum(sales_qty)
* stock level by the end of the week : inventory_on_hand_qty by the end of the week (only the stock level at the end day of the week)
* inventory on order level by the end of the week: ordered_inventory_qty by the end of the week (only the ordered stock quantity at the end day of the week)
* total cost of the week: Sum(sales_cost)
* the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)
* total Low Stock Impact: sum (out_of+stock_flg + Low_Stock_flg)
* potential Low Stock Impact: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)
* no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)
* low Stock Instances: Calculate how many times of Low_Stock_Flg in a week
* no Stock Instances: Calculate then how many times of out_of_Stock_Flg in a week
* how many weeks the on hand stock can supply: (inventory_on_hand_qty at the end of the week) / sum(sales_qty)

The fact table will be sent to another S3 output bucket as a parquet file along with additional dimension tables.

## 5. AWS Glue & Athena
Glue will crawl the S3 bucket and create a table within Athena based on the schema specified. Using a modified dockerfile, I'm able to configure Athena with Apache SuperSet.

## 6. SuperSet
The visualizations outline the high-level revenue and expenses for the company and also shed light on potential stores that are being mismanaged.
![image](https://github.com/umergh7/Walmart-Data-End-to-End-Pipeline/assets/117035545/83d13282-bfb3-4dcb-8d66-9f7c9c1d839e)




