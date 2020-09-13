import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.log4j._

import scala.io.{Codec, Source}

object PopularMoviesNames {
    case class PopularMovies(UserID:Int, MovieID:Int, Rating:Int, TimeStamp:Long)

    def loadMoviesNames() : Map[Int, String] = {
        implicit val codec : Codec = Codec("ISO-8859-1")

        var moviesNames: Map[Int, String] = Map()

        val lines = Source.fromFile("../ml-100k/u.item")

        for (line <- lines.getLines()) {
            val fields = line.split('|')
            if (fields.length > 1) {
                moviesNames += (fields(0).toInt -> fields(1)) 
            }
        }
        lines.close()

        moviesNames
    }

    def main (args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C://temp")
            .getOrCreate()

        val nameDict = spark.sparkContext.broadcast(loadMoviesNames())

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
        val lookUpName: Int => String = (MovieID: Int) => {
            nameDict.value(MovieID)
        }

        val lookUpNameUDF = udf(lookUpName)

        val moviesWithNames = mostPopular.withColumn("MovieTitle", lookUpNameUDF(col("MovieID")))

        val sortedMoviesWithNames = moviesWithNames.orderBy($"Occurrences".desc).show(moviesWithNames.count().toInt ,truncate=false)

        // sortedMoviesWithNames.show(sortedMoviesWithNames.count.toInt, truncate = false)

        spark.stop()
        
    }
}