from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit
from pyspark.sql import functions as F


def main():
    #Spark session
    spark = SparkSession.builder \
        .appName("CsvToPostgres") \
        .config("spark.master", "local") \
        .getOrCreate()

    #Load the csv
    csvPath = "CarSales.csv"  # Chemin du fichier CSV
    df = spark.read.option("header", "true").csv(csvPath)

    #Creation of the car Table
    df_car = df.select('Car_id', 'Company', 'Model', 'Engine', 'Transmission', 'Color', 'Price ($)', 'Body Style').dropDuplicates()
    #Creation of client Table
    #New Client_id column by concatenate 'Customer Name' and 'Phone'
    df = df.withColumn("Client_id", concat(col("Customer Name"), lit("_"), col("Phone").cast("string")))
    df_client = df.select('Client_id', 'Customer Name', 'Gender', 'Annual Income', "Phone").dropDuplicates()
    #Creation of Dealer Table
    df_dealer = df.select('Dealer_No ', 'Dealer_Name', 'Dealer_Region').dropDuplicates()
    #Creation of transaction Table
    #New transaction_id column by concatenate 'Client_id', 'Car_id' and 'Dealer_No '
    df = df.withColumn("transaction_id", concat(col("Client_id"), lit("_"), col("Car_id"), lit("_"), col("Dealer_No ")))
    df_transaction = df.select('transaction_id', 'Client_id', 'Car_id', 'Dealer_No ', 'Date')

    #Connection to postgresql database
    jdbcUrl = "jdbc:postgresql://localhost:5432/car_sales"
    dbProperties = {"user": "usr", "password": "usr"}

    #Saving the tables on the database
    df_car.write.mode("overwrite").jdbc(url=jdbcUrl, table="car_data", properties=dbProperties)
    df_client.write.mode("overwrite").jdbc(url=jdbcUrl, table="client_data", properties=dbProperties)
    df_dealer.write.mode("overwrite").jdbc(url=jdbcUrl, table="dealer_data", properties=dbProperties)
    df_transaction.write.mode("overwrite").jdbc(url=jdbcUrl, table="transaction_data", properties=dbProperties)

    spark.stop()

if __name__ == "__main__":
    main()
