import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math._

object FilteringExamples {

    def parseLine(line: String) = {
        val fields = line.split(",")
        val stationID = fields(0)
        val entryType  = fields(2)
        val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
        (stationID, entryType, temperature)
    }

    def main(args: Array[String]) {

        Logger.getLogger("org").setLevel(Level.ERROR)

        val sc = new SparkContext("local[*]", "FilteringExamples")
        
        //FILTERING EXAMPLES
        // val filteredValues = rdd.filter(x => x._2 == "Some_value")
                  
        val lines  = sc.textFile("../1800.csv")
        val parsedLines = lines.map(parseLine)
        //val minTemps = parsedLines.filter(x => x._2 == "TMIN")
        val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
        val stationTemps = /*minTemps.map(x => (x._1, x._3.toFloat))*/maxTemps.map(x => (x._1, x._3.toFloat))
        val stationReduceds = stationTemps.reduceByKey((x, n) => max(x,n))
        
        
        for (station <- stationReduceds) {
            val stat = station._1
            val temp = station._2
            val formatTemp = f"$temp%.2f F"
            println(s"$stat maximum temperature: $formatTemp")
        }
        


    }
}