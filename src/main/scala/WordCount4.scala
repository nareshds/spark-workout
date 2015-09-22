import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ndharmasoth on 22-09-2015.
 *
 * Sort the words by their count
 * RDD Function used: sortBy(value)
 */
object WordCount4 {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Word count 4")
    val sc = new SparkContext(conf)
    try{
      val input = sc.textFile("D:/spark-workout/data/inputs")
      val output = "D:/spark-workout/data/outputs/WordCount4/"

      val lines = input.map(line => line.toLowerCase)
      lines.cache()

      //TODO: sort by descending count values, improve sortBy method call
      val sortCount = lines.flatMap(allWords => allWords.split(" "))
                            .filter(filteredWords => !(filteredWords.matches(".*\\|.*") || filteredWords.startsWith("(") ||
                                    filteredWords.startsWith(",") || filteredWords.startsWith(".") ||
                                    filteredWords.startsWith("?")))
                            .map(word => (word, 1))
                            .reduceByKey((x,y) => x + y)
                            .sortBy(_._2)
      println(s"******writing output to $output ************")
      sortCount.saveAsTextFile(output)
    }finally {
      sc.stop()
    }
  }
}
