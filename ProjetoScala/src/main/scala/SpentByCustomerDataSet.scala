import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._

object  SpentByCustomerDataSet {
    case class Customer(ID:Int, cod:Int, spent:Float)
    
    def main(args: Array[String]) {

        Logger.getLogger("org").setLevel(Level.ERROR)

        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C://temp")
            .getOrCreate()
        
        val customerSchema = new StructType()
            .add(name = "ID", IntegerType, nullable=true)
            .add(name = "cod", IntegerType, nullable=true)
            .add(name = "spent", FloatType, nullable=true)
        
        import spark.implicits._
        val ds = spark.read.schema(customerSchema).csv("../customer-orders.csv").as[Customer]

        val customersAndSpents = ds.select($"ID", $"spent")
        val spentByCustomer = ds.groupBy($"ID")
                                .agg(
                                    sum($"spent").alias("totalSpent")
                                )
                                .orderBy($"totalSpent".desc)
                                .show()

        spark.stop()


    }

}