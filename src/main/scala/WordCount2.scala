import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ndharmasoth on 20-09-2015.
 *
 * Given any text document, get the number of occurrences of each word in ascending order of word
 * The below code uses one of the OrderedRDDFunctions of spark.
 * OrderedRDDFunction supports filterByRange, repartitionAndSortWithinPartitions and SortByKey
 * Here we use SortByKey member of the OrderedRDDFunction
 */
object WordCount2 {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Word Count 2")
    val sc = new SparkContext(conf)
    try{
      val input = sc.textFile("D:/spark-workout/data/inputs/")
      val output = "D:/spark-workout/data/outputs/WordCount2/"

      val lines = input.map(line => line.toLowerCase)
      lines.cache()

      val wc = lines.flatMap(allWords => allWords.split(" "))
        .filter(filteredWords => !filteredWords.matches(".*\\|.*"))
        .map(word => (word, 1))
        .reduceByKey((x,y) => x + y)
        .sortByKey(true)

      println("s******Writing output to $output**********")
      wc.saveAsTextFile(output)
    }finally {
      sc.stop()
    }
  }
}
