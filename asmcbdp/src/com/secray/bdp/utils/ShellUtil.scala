package  com.secray.bdp.utils

import sys.process._
import com.secray.bdp.log.Logging
import java.io.File
import scala.reflect.io.Directory
import java.net.URLDecoder
import org.apache.hadoop.conf.Configuration


object ShellUtil extends Logging{

   val javaHome = SysUtil.getJavaHome()
   val scalaHome =  SysUtil.getScalaHome()
   //获取当前开发项目的目录
   val clsPaths = URLDecoder.decode(ShellUtil.getClass.getResource("/").getPath(),"UTF-8")
   //由于getResource获取项目目录的分隔符是"/",需把目录的分隔符由"/"替换成"\"
   var clsPath = clsPaths.substring(1,clsPaths.length()).replace("/", "\\")


  def  generateJar(uploadFile : String) = {
      //获取上传文件的目录
      val uploadPath = uploadFile.substring(0,uploadFile.lastIndexOf("\\"))

      //如果不存在，则创建
      val file = new File(uploadPath)
      if( !file.exists() ){
        file.mkdirs()
      }
      val addJar = "jar cf "+uploadFile+" -C "+clsPath +" ."
      log.debug("generateJar : "+clsPath+" :\n "+ addJar)
      addJar!
  }
  def compile(destPaht : String,isQuick : Boolean = true) ={
  //  val clsPath = clsPaths.substring(1,clsPaths.length()).replaceAll("/", "\\")
     val pSep = SysUtil.getFileSeparator()
     val pathSep = SysUtil.getPathSeparator()

     val srcPath = clsPath.replace(pSep+"bin"+pSep, pSep+"src"+pSep)
     var compileCmd = "fsc"
     if( !isQuick) compileCmd="scalac"
     val scalaLibs = FileUtil.getSubDirFiles(new File(scalaHome+pSep+"lib")).filter{ f =>
                                  if(f.getName.toUpperCase().indexOf(".JAR") > -1 && f.isFile() )
                                    true
                                  else
                                    false
                                }
     val otherLibs = FileUtil.getSubDirFiles(new File(clsPath.replace(pSep+"bin"+pSep, pSep+"spLib"+pSep))).filter{ f =>
                                  if(f.getName.toUpperCase().indexOf(".JAR") > -1 && f.isFile() )
                                    true
                                  else
                                    false
                                }
     val compileCmdLibs = compileCmd + " -cp ,"+ pathSep +
                         scalaLibs.mkString(pathSep) +
                         pathSep +
                         otherLibs.mkString(pathSep)

     for(file <- FileUtil.getSubDirFiles(new File(srcPath))){
         val compileFile = scalaHome+pSep+"bin"+pSep+compileCmdLibs+" -d "+ destPaht +" "+file.getPath()+file.getName
         log.info("compile : "+compileFile)
        // log.info("compile : "+file.getPath() + " : " + file.getName)
     }
  }


   /**
   * 函数名    : genUploadJar
   * 功   能    : 把开发环境下的class打成jar包，并放到HFDS上
   * 参   数    : @param conf
   * 参   数    : @param localFilePath  : 本地目录(jar文件也生成到这个目录下)
   * 参   数    : @param remoteFilePath : HDFS上的目录
   * 返回值	 : 无
   * 编写者    : root
   * 编写时间 : 2017年02月09日 下午15:38:26
   */
  def genUploadJar(localFilePath : String, remoteFilePath : String) = {
    val conf = new Configuration()

    ShellUtil.generateJar(localFilePath)
    log.info("genUploadJar : "+localFilePath+" 生成完毕!")
    HDFSUtil.put(conf, localFilePath, remoteFilePath)
    log.info("genUploadJar : "+localFilePath+" 上传到远程目录["+remoteFilePath+"]完毕!")

  }
  def main(args: Array[String]): Unit = {
    //ShellUtil.lb()
       //ShellUtil.generateJar("d:\\lb\\spG01.jar")
       //ShellUtil.compile("d:/lb")

     val srcPath = "d:\\yf\\ljr\\spG01.jar"
     val destPath = "hdfs://192.168.12.9:8020/apps/spark"
     ShellUtil.genUploadJar(srcPath, destPath)
     //println( URLDecoder.decode(ShellUtil.getClass.getResource("\\").getPath(),"UTF-8") )

   }
}
