import org.apache.spark._
import org.apache.log4j._

object MapFlatMapExercises {

    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

        val sc = new SparkContext("local[*]", "MapFlatMapExercises")
        val lines = sc.textFile("../book.txt")
        
        val rdd = lines.flatMap(x => x.split("\\W+"))

        val normalizedWords = rdd.map(x => x.toLowerCase())

        val wordReduced = normalizedWords.map(x => (x , 1)).reduceByKey((x,y) => x + y)

        val wordsSorted = wordReduced.map(x => (x._2, x._1)).sortByKey(true, 1)
        
        for (r <- wordsSorted) {
            val value = r._1
            val word = r._2
            println(s"$word: $value")
        }
    }
}