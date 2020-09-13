import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object FriendsByAgeDataSet {

    case class Person(ID:Int, name:String, age:Int, numFriends:Int)

    def mapper (lines: String) : Person = {
        val content = lines.split(",")
        val person: Person = Person(content(0).toInt, content(1).toString, content(2).toInt, content(3).toInt)
        return person
    }

    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C://temp")
            .getOrCreate()
        
        val lines = spark.sparkContext.textFile("../fakefriends.csv")
        val people = lines.map(mapper)
        
        import spark.implicits._
        val p = people.toDS

        p.printSchema()

        //val friendsNames = p.select($"name").show()

        
        val friendsByAge = p.groupBy($"age")
                            .agg(
                                round(avg($"numFriends"),scale=2).as("media")
                            )
                            .orderBy($"media".desc)
                            .show()

        spark.stop()    
    }

}