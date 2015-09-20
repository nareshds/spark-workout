import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ndharmasoth on 16-09-2015.
 *
 * Given any document, get the number of occurrence of each word in the document
 */
object WordCount {
  def main(args: Array[String]):Unit={
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    try{
      val input = sc.textFile("D:/spark-workout/data/inputs")
      val output = "D:/spark-workout/data/outputs/WordCount/"

      val lines = input.map(line => line.toLowerCase)
      lines.cache()

      val wc = lines.flatMap(allWords => allWords.split(" "))
                    .filter(filteredWords => !filteredWords.matches(".*\\|.*"))
                    .map(word => (word, 1))
                    .reduceByKey((x, y) => x + y)

      println(s"***********Writing output to: $output**********")
      wc.saveAsTextFile(output)
    }finally {
      sc.stop()
    }
  }
}
