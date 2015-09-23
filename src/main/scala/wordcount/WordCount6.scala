package wordcount

import org.apache.spark.{SparkContext, SparkConf}
import util.{FileUtil, CommandLineOptions}

/**
 * Created by ndharmasoth on 23-09-2015.
 */
object WordCount6 {
  def main(args: Array[String]): Unit = {
    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("D:/spark-workout/data/inputs/"),
      CommandLineOptions.outputPath("D:/spark-workout/data/outputs/WordCount6/"),
      CommandLineOptions.master("local[2]"),
      CommandLineOptions.quiet
    )

    val argz = options(args.toList)
    val master = argz("master")
    val quiet = argz("quiet").toBoolean
    val in = argz("input-path")
    val out = argz("output-path")
    if(master.startsWith("local")){
      if(!quiet) println(s"#####Deleting existing output files(if any): $out####")
      FileUtil.rmrf(out)
    }
    val conf = new SparkConf().setAppName("Word Count 6").setMaster(master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    try{
      val input = sc.textFile(in).map(lines => lines.toLowerCase())

      val wc = input.flatMap(allwords => allwords.split(" "))
                    .filter(filteredWords => !filteredWords.matches(".*\\|.*"))
                    .map(word => (word, 1))
                    .reduceByKey((x, y) => x + y)

      if(quiet == false) println(s"**********Writing output to $out******")
      wc.saveAsTextFile(out)
    }finally {
      sc.stop()
    }
  }
}
