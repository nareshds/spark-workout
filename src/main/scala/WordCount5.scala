import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ndharmasoth on 22-09-2015.
 *
 * Group the word count pairs by count.
 * As of now secondary sort is not available in spark, so key and value are swapped
 * sorted on key after swap.
 */
object WordCount5 {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Word count 5")
    val sc = new SparkContext(conf)
    try{
      val input = sc.textFile("D:/spark-workout/data/inputs/")
      val output = "D:/spark-workout/data/outputs/WordCount5/"

      val lines = input.map(line => line.toLowerCase)
      lines.cache()

      val groupByCount = lines.flatMap(allWords => allWords.split(" "))
                              .filter(filteredWords => !(filteredWords.matches(".*\\|.*") || filteredWords.startsWith("(") ||
                                      filteredWords.startsWith(",") || filteredWords.startsWith(".") ||
                                      filteredWords.startsWith("?")))
                              .map(word => (word, 1))
                              .reduceByKey((x,y) => x + y)
                              .map(_.swap)
                              .groupByKey()
                              .sortByKey(true)
      println(s"*******Writing output to $output**********")
      groupByCount.saveAsTextFile(output)
    }finally {
      sc.stop()
    }
  }
}
