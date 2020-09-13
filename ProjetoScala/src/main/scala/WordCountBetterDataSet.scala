import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._



object WordCountBetterDataSet {
    case class Book(value:String)

    def main (args: Array[String]) {

        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C://temp")
            .getOrCreate()
        


        import spark.implicits._
        val lines = spark.sparkContext.textFile("../book.txt")
        val words  = lines.flatMap(x => x.split("\\W+"))
        val w = words.toDS

        val wCounts = w.groupBy($"value")
                            .agg(
                                count($"value").alias("ocorrencias")
                            )
                            .orderBy($"ocorrencias".desc).show()
       
        
    }

}