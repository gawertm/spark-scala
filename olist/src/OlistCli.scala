import org.apache.spark.sql.SparkSession
object OlistCli {
  
    def listDelayedDeliveries(spark: SparkSession) = {
        val df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "true").load("data/file.csv")
        df.show
    }

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Spark Olist").config("spark.master", "local")  
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }

    def main(args: Array[String]) = {
        run(listDelayedDeliveries _)
    }
}