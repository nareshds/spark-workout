package joins

import java.io.File

import org.apache.spark.{SparkContext, SparkConf}
import util.FileUtil

/**
 * Created by ndharmasoth on 29-09-2015.
 */
object InnerJoin {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Inner Join").setMaster("local")
    val sc = new SparkContext(conf)
    try{
      val left_table = sc.textFile("D:/spark-workout/data/inputs/bible.txt")
                          .map{line => val ary =line.split("\\s*\\|\\s*")
                                      (ary(0), (ary(1), ary(2), ary(3)))}
      val right_table = sc.textFile("D:/spark-workout/data/inputs/joins/")
                          .map{line => val ary = line.split(",")
                              (ary(0), ary(1))
                            }

      left_table.cache
      right_table.cache

      val verses = left_table.join(right_table)
                             .map{
                                    case (_, ((chapter, verse, text), fullBookName)) => (fullBookName, chapter, verse, text)
                              }
      val output = "D:/spark-workout/data/outputs/joins/"
      if(new File(output).exists()){
        println(s"Deleting file $output")
        FileUtil.rmrf(output)
      }
      verses.saveAsTextFile(output)
    }finally {
      sc.stop()
    }
  }
}
