import org.apache.spark.sql._
import org.apache.log4j._

object FriendsByAgeSQL {
    
    case class Person(id:Int, name:String, age:Int, friends:Int)

    def main(args : Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .getOrCreate()

        import spark.implicits._
        val people = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path="../fakefriendsColumns.csv")
            .as[Person]

        // schemaPeople.printSchema()

        // schemaPeople.createOrReplaceTempView("people")

        // val teenagers = spark.sql("SELECT * FROM people WHERE age>= 13 and age <= 19")

        // val results = teenagers.collect()

        // results.foreach(println)

        // spark.stop()

        people.printSchema()
        people.select(col="name").show()
        people.groupBy(col1="age").count().show()
        people.select(people("name"), people("age") + 10).show()


        spark.stop()
    }
}