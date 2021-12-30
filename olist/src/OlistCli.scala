import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object OlistCli {
  
    def listDelayedDeliveries(spark: SparkSession) = {
        //define schema to import directly as Timestamps
        val schema = new StructType()
        .add("order_id", StringType, true)
        .add("customer_id", StringType, true)
        .add("order_status", StringType, true)
        .add("order_purchase_timestamp", TimestampType, true)
        .add("order_approved_at", TimestampType, true)
        .add("order_delivered_carrier_date", TimestampType, true)
        .add("order_delivered_customer_date", TimestampType, true)
        .add("order_estimated_delivery_date", TimestampType, true)

        // error was thrown if not specified the .format with fully qualified class name as well as .load. csv exists in 2 classes (CSVFileFormat as well as V2)
        // imported csv files from kaggle olist dataset
        // additionally created a small timezones csv file manually. Because in brazil, there are only 27 states and each have only 1 timezone. then the state name is mapped to customer_state in customers dataset
        var ordersdf = spark
            .read
            .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
            .option("header", "true").schema(schema)
            .load("data/olist_orders_dataset.csv")
        ordersdf = ordersdf.filter(col("order_status") === "delivered").filter(col("order_delivered_customer_date").isNotNull) //filter out cancelled deliveries and no delivery dates
        
        val customersdf = spark
            .read
            .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
            .option("header", "true").load("data/olist_customers_dataset.csv")
       
        val timezonesdf = spark
            .read
            .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
            .option("header", "true")
            .option("delimiter", ";")
            .load("data/timezones.csv")

        val joindf = customersdf.join(timezonesdf, "customer_state") //joining customers dataset with timezones for later utc conversion
        var masterdf = ordersdf.join(joindf, "customer_id") // join joined customer datset with orders dataset

        //using to_utc_timestamp from spark sql library. timezones are taken from the joined timezones csv file
        masterdf = masterdf.withColumn("order_time_utc", expr("to_utc_timestamp(order_purchase_timestamp,'America/Sao_Paulo')"))
        masterdf = masterdf.withColumn("delivery_time_utc", expr("to_utc_timestamp(order_delivered_customer_date, customer_timezone)"))
        masterdf = masterdf.withColumn("delivery_delay_in_days", datediff(col("delivery_time_utc"), col("order_time_utc")))
        
        //filter for delivery that are delayed more than 10 days
        masterdf = masterdf.filter(col("delivery_delay_in_days") > 10).sort(desc("delivery_delay_in_days"))
        masterdf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").option("delimiter", ",").csv("results") //coalesce into 1 csv file and overwrite if file already exists
    }

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Spark Olist").config("spark.master", "local").config("spark.executor.memory" , "8g")
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }

    def main(args: Array[String]) = {
        run(listDelayedDeliveries _)
    }
}