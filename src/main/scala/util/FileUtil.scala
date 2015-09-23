package util

import java.io.File

/**
 * Created by ndharmasoth on 23-09-2015.
 */
object FileUtil {
  case class FileOperationError(msg: String) extends RuntimeException(msg)

  def rmrf(root: String): Unit ={
    rmrf(new File(root))
  }

  def rmrf(root: File): Unit ={
    if(root.isFile) root.delete()
    else if(root.exists()){
      root.listFiles foreach rmrf
      root.delete()
    }
  }
}
