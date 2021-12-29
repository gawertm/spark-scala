import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger;

object OlistCli {
  
    def listDelayedDeliveries(spark: SparkSession) = {
        val df = spark.read.option("header", true).csv("data/file.csv")
        df.show
    }

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Spark Olist")  
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }

    def main(args: Array[String]) = {
        println("Olist Cli: XXX")
        run(listDelayedDeliveries _)
    }
}