package  com.secray.bdp.utils

import java.io.File
import scala.reflect.io.Directory

object FileUtil {

  //def getSubDir() :

    def getSubDir(dir:File):Iterator[File] ={
      val subPaths = dir.listFiles().filter(_.isDirectory())
      subPaths.toIterator ++ subPaths.toIterator.flatMap(getSubDir _)
    }


   def getSubDirFiles(dir:File):Iterator[File] ={
      val dirs = dir.listFiles().filter(_.isDirectory())
      val files = dir.listFiles().filter(_.isFile())
      files.toIterator ++ dirs.toIterator.flatMap(getSubDirFiles _)
  }


  def main(args: Array[String]): Unit = {

   }
}
