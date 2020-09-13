import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByAge {

    // metodo de parse
    def parseFriedns(line: String) = {
        // fazer o um split no conteudo do arquivo, neste momento é uma string
        val fields : Array[String] =  line.split(",")
        // recolher e converter campos de idade para inteiro
        val age = fields(2).toInt
        // recolher e converter campo de número de amigos naquela idade
        val numFriends = fields(3).toInt
        (age, numFriends)
    }

    // iniciando a main
    def main (args: Array[String]) {
        // configurando Logger
        Logger.getLogger("org").setLevel(Level.ERROR)
        // iniciando o spark context
        val sc = new SparkContext("local[*]", "FriendsByAge")
        // lendo conteudo do arquivo
        val lines  = sc.textFile("../fakefriends.csv")
        // parsear o conteudo do arquivo transformando-o em RDD
        // o map simulará um loop pelas lines aplicando a função parseFriends a cada volta
        val rdd = lines.map(parseFriedns)
        // Lots going on here...
        // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
        // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
        // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
        // adding together all the numFriends values and 1's respectively.
        val totalByAgeTwo = rdd.mapValues(x => (x,1)).reduceByKey((x,n) => (x._1 + n._1, x._2 + n._2))
        val averageByAge = totalByAgeTwo.mapValues(x => (x._1 / x._2))
        // Sempre quando se encerrar as operações com um RDD aplicar o collect
        val resultados = averageByAge.collect()
        // Imprimindo os resultados
        resultados.sorted.foreach(println)

    }

}
