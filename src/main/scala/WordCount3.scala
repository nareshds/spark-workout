import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ndharmasoth on 20-09-2015.
 *
 * Take the output from the previous exercise and count the number
 * of words that start with each letter of the alphabet and each digit.
 */
object WordCount3 {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Word count3")
    val sc = new SparkContext(conf)
    try{
      val input = sc.textFile("D:/spark-workout/data/outputs/WordCount2/")
      val output = "D:/spark-workout/data/outputs/WordCount3/"

      val startCharCount = input.flatMap(char => char.charAt(1).toString)
                                .filter(specialChars => !(specialChars.equals(',') || specialChars.equals('(') ||
                                                        specialChars.equals('?') || specialChars.equals('.')))
                                .map(char => (char, 1))
                                .reduceByKey((x,y) => x + y)
                                .sortByKey(true)
      println("s*********Writing output to $output********")
      startCharCount.saveAsTextFile(output)
    }finally {
      sc.stop()
    }
  }
}
