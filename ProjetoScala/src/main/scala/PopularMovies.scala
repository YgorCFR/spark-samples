import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.log4j._


object PopularMovies {

    case class PopularMovies(UserID:Int, MovieID:Int, Rating:Int, TimeStamp:Long)

    def main (args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C://temp")
            .getOrCreate()

        val popularMoviesSchema = new StructType()
            .add(name = "UserID", IntegerType, nullable=false)
            .add(name = "MovieID", IntegerType, nullable=false)
            .add(name = "Rating", IntegerType, nullable=false)
            .add(name = "TimeStamp", LongType, nullable=false)
        
        import spark.implicits._
        val pM = spark.read.option("sep","\t").schema(popularMoviesSchema).csv("../ml-100k/u.data").as[PopularMovies]

        val mostPopular = pM.groupBy($"MovieID")
                                .agg(
                                    count($"Rating").alias("Occurrences")
                                )
                                .orderBy($"Occurrences".desc)
                                .show(50, false)
        spark.stop() 
    }
}