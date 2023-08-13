from pyspark.sql import SparkSession
import argparse
from datetime import datetime


def spark_init(spark_name):
    spark = SparkSession \
    .builder \
    .appName(spark_name) \
    .getOrCreate()
    return spark
     

#write file back to s3
#for now csv, change to parquet later
def write(df, output_file_url):
    current_date = datetime.now().strftime("%Y%m%d")
    df.repartition(1).write.mode('overwrite').option('compression', 'gzip').parquet(output_file_url + f'/date={current_date}')

def write_dim_store(store, output_file_url_dim):
    current_date = datetime.now().strftime("%Y%m%d")
    store.write\
    .option("header", "true")\
    .mode("overwrite")\
    .csv(output_file_url_dim + f"/store/date={current_date}")

def write_dim_product(product, output_file_url_dim):
    current_date = datetime.now().strftime("%Y%m%d")
    product.write\
    .option("header", "true")\
    .mode("overwrite")\
    .csv(output_file_url_dim + f"/product/date={current_date}")

def write_dim_calendar(calendar, output_file_url_dim):
    current_date = datetime.now().strftime("%Y%m%d")
    calendar.write\
    .option("header", "true")\
    .mode("overwrite")\
    .csv(output_file_url_dim + f"/calendar/date={current_date}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--spark_name', help="spark_name")
    parser.add_argument('--output_file_url', help="output file S3 bucket location.")
    parser.add_argument('--output_file_url_dim', help="output file S3 bucket location.")
    parser.add_argument('--sales', help="s3 bucket path")
    parser.add_argument('--calendar', help="s3 bucket path")
    parser.add_argument('--inventory', help="s3 bucket path")
    parser.add_argument('--product', help="s3 bucket path")
    parser.add_argument('--store', help="s3 bucket path")

    args = parser.parse_args()
    spark_name = args.spark_name
    output_file_url = args.output_file_url
    output_file_url_dim = args.output_file_url_dim
    sales = str(args.sales)
    calendar = str(args.calendar)
    inventory = str(args.inventory)
    product = str(args.product)
    store = str(args.store)

    #initializing spark
    spark=spark_init(spark_name)
    
    sales = spark.read.option("header","true").option("delimiter", ",").csv(sales)
    calendar = spark.read.option("header","true").option("delimiter", ",").csv(calendar)
    inventory = spark.read.option("header","true").option("delimiter", ",").csv(inventory)
    product = spark.read.option("header","true").option("delimiter", ",").csv(product)
    store = spark.read.option("header","true").option("delimiter", ",").csv(store)

    #now that we have the dataframes, lets complete the transformations
    
    sales.createOrReplaceTempView("sales")
    calendar.createOrReplaceTempView("calendar")
    inventory.createOrReplaceTempView("inventory")
    product.createOrReplaceTempView("product")
    store.createOrReplaceTempView("store")

    #JOIN WK_NUM AND YR_NUM
    sales = spark.sql (
    """
    SELECT
    TRANS_ID,
    PROD_KEY,
    STORE_KEY,
    TRANS_DT,
    YR_NUM,
    WK_NUM,
    TRANS_TIME,
    SALES_QTY,
    SALES_PRICE,
    SALES_AMT,
    DISCOUNT,
    SALES_COST,
    SALES_MGRN,
    SHIP_COST
    FROM sales s
    LEFT JOIN calendar c
    ON s.TRANS_DT = c.CAL_DT
    """
    )

    sales.createOrReplaceTempView("sales")

    inventory = spark.sql (
    """
    SELECT
    CAL_DT,
    YR_NUM,
    WK_NUM,
    STORE_KEY,
    PROD_KEY,
    INVENTORY_ON_HAND_QTY,
    INVENTORY_ON_ORDER_QTY,
    OUT_OF_STOCK_FLG,
    WASTE_QTY,
    PROMOTION_FLG,
    NEXT_DELIVERY_DT
    FROM inventory i
    LEFT JOIN calendar c
    USING(CAL_DT)
    """
    )

    inventory.createOrReplaceTempView("inventory")

    #  Calculate total sales quantity of a product : Sum(sales_qty)
    # Calculate total sales amount of a product : Sum(sales_amt)

    base1 = spark.sql(
    """
    SELECT
    YR_NUM,
    WK_NUM,
    PROD_KEY,
    STORE_KEY,
    SUM(SALES_QTY) AS total_sales_qty,
    SUM(SALES_AMT) AS total_sales_amt
    FROM
    sales
    GROUP BY
    YR_NUM,
    WK_NUM,
    PROD_KEY,
    STORE_KEY
    """
    )

    # calculate average sales Price: Sum(sales_amt)/Sum(sales_qty)
    base2 =base1.withColumn("avg_sales_price", base1['total_sales_amt']/base1['total_sales_qty'])

    # stock level by then end of the week : stock_on_hand_qty by the end of the week (only the stock level at the end day of the week)
    base3 = spark.sql (
        """
        WITH query AS (
        SELECT CAL_DT, YR_NUM, WK_NUM, store_key, prod_key, inventory_on_hand_qty, RANK() OVER (PARTITION BY STORE_KEY, PROD_KEY, YR_NUM, WK_NUM ORDER BY CAL_DT DESC) AS Rank1
        FROM inventory
        )

        SELECT YR_NUM, WK_NUM, STORE_KEY, PROD_KEY, inventory_on_hand_qty
        FROM query
        WHERE rank1=1
        """
    )

    # store on Order level by then end of the week: ordered_stock_qty by the end of the week (only the ordered stock quantity at the end day of the week)


    base4 = spark.sql (
        """
        WITH query AS (
        SELECT CAL_DT, YR_NUM, WK_NUM, store_key, prod_key, inventory_on_order_qty, RANK() OVER (PARTITION BY STORE_KEY, PROD_KEY, YR_NUM, WK_NUM ORDER BY CAL_DT DESC) AS Rank1
        FROM inventory
        )

        SELECT YR_NUM, WK_NUM, STORE_KEY, PROD_KEY, inventory_on_order_qty
        FROM query
        WHERE rank1=1
        """
    )

    # total cost of the week: Sum(cost_amt)
    base5 = spark.sql(
        """
        SELECT
        YR_NUM,
        WK_NUM,
        PROD_KEY,
        STORE_KEY,
        (SUM(SALES_COST) + SUM(SHIP_COST)) AS Total_Weekly_Cost
        FROM sales
        GROUP BY 1,2,3,4
        """
    )

    # the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)

    base6 = spark.sql (
        """
        SELECT
        YR_NUM,
        WK_NUM,
        STORE_KEY,
        PROD_KEY,
        (100 * SUM(OUT_OF_STOCK_FLG))/7 AS Store_In_Stock_percent
        FROM inventory
        GROUP BY
        1,2,3,4
        """
    )


    # calculate low stock flg
    # potential Low Stock Impact: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)

    base7 = spark.sql (
        """
        WITH Query AS (
        SELECT
            CAL_DT,
            s.YR_NUM,
            s.WK_NUM,
            s.STORE_KEY,
            s.PROD_KEY,
            INVENTORY_ON_HAND_QTY,
            SALES_QTY,
            SALES_AMT,
            (SALES_AMT - INVENTORY_ON_HAND_QTY) AS temp,
            CASE
                WHEN INVENTORY_ON_HAND_QTY < SALES_QTY THEN 1
                ELSE 0
            END AS LOW_STOCK_FLG
        FROM sales s
        JOIN inventory i
            ON TRANS_DT = CAL_DT
            AND s.YR_NUM = i.YR_NUM
            AND s.WK_NUM = i.WK_NUM
            AND s.STORE_KEY = i.STORE_KEY
            AND s.PROD_KEY = i.PROD_KEY
    )

    SELECT
        YR_NUM,
        WK_NUM,
        STORE_KEY,
        PROD_KEY,
        LOW_STOCK_FLG,
        CASE
            WHEN LOW_STOCK_FLG = 1 THEN SUM(temp)
            ELSE 0
        END AS potential_low_stock_impact
    FROM Query
    GROUP BY YR_NUM, WK_NUM, STORE_KEY, PROD_KEY, LOW_STOCK_FLG

        """
    )

    base7.createOrReplaceTempView("base7")
    
    base8 = spark.sql (
        """
    WITH query AS (
        SELECT
            YR_NUM,
            WK_NUM,
            STORE_KEY,
            PROD_KEY,
            LOW_STOCK_FLG,
            OUT_OF_STOCK_FLG,
            LOW_STOCK_FLG + OUT_OF_STOCK_FLG as temp
        FROM base7 b
        JOIN inventory i
        USING(YR_NUM, WK_NUM, STORE_KEY, PROD_KEY)
    )

    SELECT
        YR_NUM,
        WK_NUM,
        STORE_KEY,
        PROD_KEY,
        LOW_STOCK_FLG,
        OUT_OF_STOCK_FLG,
        SUM(temp) AS total_low_stock_impact
    FROM query
    GROUP BY YR_NUM, WK_NUM, STORE_KEY, PROD_KEY, LOW_STOCK_FLG, OUT_OF_STOCK_FLG
        """
    )

    # no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)

    base9 = spark.sql(
        """
        WITH query AS (
        SELECT
        CAL_DT,
        s.YR_NUM,
        s.WK_NUM,
        s.STORE_KEY,
        s.PROD_KEY,
        OUT_OF_STOCK_FLG,
        SALES_AMT
        FROM sales s
        JOIN inventory i
        ON TRANS_DT = CAL_DT
        AND s.YR_NUM = i.YR_NUM
        AND s.WK_NUM = i.WK_NUM
        AND s.STORE_KEY = i.STORE_KEY
        AND s.PROD_KEY = i.PROD_KEY
    )

    SELECT
    YR_NUM,
    WK_NUM,
    STORE_KEY,
    PROD_KEY,
    CASE
        WHEN OUT_OF_STOCK_FLG = 1
        THEN SUM(sales_AMT)
        ELSE 0
    END AS no_stock_impact
    FROM query
    GROUP BY YR_NUM, WK_NUM, STORE_KEY, PROD_KEY, OUT_OF_STOCK_FLG

        """
    )

    # low Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)

    base10 = spark.sql(
        """
        SELECT
        YR_NUM,
        WK_NUM,
        STORE_KEY,
        PROD_KEY,
        LOW_STOCK_FLG,
        SUM(LOW_STOCK_FLG) AS low_stock_instance
        FROM base7
        GROUP BY YR_NUM, WK_NUM, STORE_KEY, PROD_KEY, LOW_STOCK_FLG
        ORDER BY 1,2,3,4
        """
    )

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # Assuming you have a DataFrame named base8

    # Performing the groupBy and sum operation
    window_spec = Window.partitionBy('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY')
    base11 = base8.withColumn('no_stock_instances', F.sum('out_of_stock_flg').over(window_spec))

    base3.createOrReplaceTempView("base3")
    
    base12 = spark.sql(
        """

        WITH query AS (
        SELECT
        YR_NUM,
        WK_NUM,
        STORE_KEY,
        PROD_KEY,
        Inventory_on_hand_qty,
        sales_qty
        FROM base3
        join sales s
        USING (YR_NUM,
        WK_NUM,
        STORE_KEY,
        PROD_KEY)
        )

        SELECT
        YR_NUM,
        WK_NUM,
        STORE_KEY,
        PROD_KEY,
        Inventory_on_hand_qty/(sum(sales_qty)) AS Weeks_on_hand_supply
        FROM query
        GROUP BY YR_NUM, WK_NUM, STORE_KEY, PROD_KEY, Inventory_on_hand_qty
        """
    )

    #lets join into one df
    # Columns required:
    # YR_NUM, WK_NUM, PROD_KEY, STORE_KEY, Total_sales_qty, total_sales_amt, avg_sales_price
    df = base1.join(base2.select('YR_NUM', 'WK_NUM', 'PROD_KEY', 'STORE_KEY', 'avg_sales_price'),
                    on=['YR_NUM', 'WK_NUM', 'PROD_KEY', 'STORE_KEY'],
                    how='left')
    #inventory on hand
    df = df.join(base3.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'inventory_on_hand_qty'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')

    #inventory on order qty
    df = df.join(base4.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'inventory_on_order_qty'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')

    #total weekly cost
    df = df.join(base5.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'Total_Weekly_Cost'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')

    #store in stock percent
    df = df.join(base6.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'Store_In_Stock_percent'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')


    # low_stock_flg and potential low stock impact
    df = df.join(base7.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'LOW_STOCK_FLG', 'potential_low_stock_impact'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')

    # total low stock impact
    df = df.join(base8.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'total_low_stock_impact'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')

    #no stock impact
    df = df.join(base9.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'no_stock_impact'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')

    #low stock instances
    df = df.join(base10.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'low_stock_instance'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')


    #no stock instances
    df = df.join(base11.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'no_stock_instances'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')


    #Weeks_on_hand_supply
    df = df.join(base12.select('YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY', 'Weeks_on_hand_supply'),
                on=['YR_NUM', 'WK_NUM', 'STORE_KEY', 'PROD_KEY'],
                how='left')
    


    write(df, output_file_url)
    write_dim_store(store, output_file_url_dim)
    write_dim_product(product, output_file_url_dim)
    write_dim_calendar(calendar, output_file_url_dim)







