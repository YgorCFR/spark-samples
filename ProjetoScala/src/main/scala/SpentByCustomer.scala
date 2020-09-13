import org.apache.spark._
import org.apache.log4j._


object SpentByCustomer {

    def parseDetails(lines: String) = {
        val content = lines.split(",")
        val id = content(0).toString
        val value = content(2).toFloat
        (id, value)
    }

   

    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val sc = new SparkContext("local[*]", "SpentByCustomer")

        val lines = sc.textFile("../customer-orders.csv")

        val rdd = lines.map(parseDetails)

        val totals = rdd.reduceByKey((x, s) => (x + s))

        val sortedTotals = totals.map(x => (x._2, x._1)).sortByKey(false, 1)

        val results = sortedTotals.collect()

        for(r <- results) {
            val id = r._2
            val value = f"US$$ ${r._1}%.2f"

            println(s"person: $id, spent: $value")
        }
    }
}