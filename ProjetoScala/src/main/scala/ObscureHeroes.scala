import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType }
import org.apache.log4j._


object ObscureHeroes {
    case class SuperHeroNames(id: Int, name: String)
    case class SuperHero(value: String)

    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
                    .builder
                    .appName("SparkSQL")
                    .master("local[*]")
                    .config("spark.sql.warehouse.dir", "file:///C://temp")
                    .getOrCreate()
        
        val shNamesSchema = new StructType()
            .add(name = "id", IntegerType, nullable=false)
            .add(name = "name", StringType, nullable=false)

        import spark.implicits._
        val hN = spark.read.schema(shNamesSchema).option("sep", " ").csv("../Marvel-names.txt").as[SuperHeroNames]

        val lines = spark.read.text("../Marvel-graph.txt").as[SuperHero]

        val connections = lines.withColumn("id", split(col("value"), " ")(0))
                               .withColumn("connections", size(split(col("value"), " ")) - 1)
                               .groupBy("id").agg(
                                   sum("connections").alias("connections")
                               )

        val joinNamesAndGraph = connections.join(hN, connections("id") === hN("id"), "inner")
                                    .orderBy($"connections".asc)
        
        joinNamesAndGraph.show()
        
        spark.stop()



    }

}