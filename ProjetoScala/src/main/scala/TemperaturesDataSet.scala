import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object TemperaturesDataSet {
    case class Temperature(stationID:String, date:Int, measureType:String, temperature:Float)

    def main (args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C://temp")
            .getOrCreate()

        val temperatureSchema = new StructType()
            .add(name = "stationID", StringType, nullable=true)
            .add(name = "date", IntegerType, nullable=true)
            .add(name = "measureType", StringType, nullable=true)
            .add(name = "temperature", FloatType, nullable=true)
        
        import spark.implicits._
        val ds = spark.read.schema(temperatureSchema).csv("../1800.csv").as[Temperature]

        val minTemps = ds.filter(condition=$"measureType" === "TMIN")
        val stationsByTemps = ds.select($"stationID", $"temperature")
        val minByStation = ds.groupBy($"stationID")
            .agg(min("temperature").alias("minimo"),
                 max("temperature").alias("maximo")     
            ).show()

        spark.stop()
    }
}