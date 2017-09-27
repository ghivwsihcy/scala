package  com.secray.bdp.utils

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.OutputStreamWriter

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.secray.bdp.log.Logging
import java.net.URI
import java.io.ByteArrayOutputStream
import org.apache.hadoop.io.IOUtils




object HDFSUtil extends Logging {

    /**
      *
     * 函数名    : move
     * 功   能    :  在HDFS上移动文件
     * 参   数    : @param conf
     * 参   数    : @param srcFileName
     * 参   数    : @param destFileName
     * 参   数    : @return
     * 参   数    : @throws IOException    设定文件
     * 返回值	 : boolean    返回类型
     * 编写者    : root
     * 编写时间 : 2016年11月7日 下午4:09:26
      */
  def move( conf : Configuration,  srcFileName : String,  destFileName : String) :Boolean = {
       val fs = FileSystem.get(conf)
       val srcFile = new Path(srcFileName)
       val destFile = new Path(destFileName)
      //目标文件存在，则删除
       if( fs.exists(destFile) )
       {
      	 fs.delete(destFile, false)
       }
       val destFilePath = destFileName.substring(0, destFileName.lastIndexOf("/"))
       val destPath = new Path(destFilePath)
     //目标文件所在目录不存在，则创建
       if( !fs.exists(destPath) )
       {
      	 fs.mkdirs(destPath);
       }

       var in : InputStream = null
    	 var reader : BufferedReader = null
    	 var writer : BufferedWriter = null
    	 var out : OutputStream = null
    	 var line : String = null


    	 try
    	 {
         in = fs.open(srcFile)
         out = fs.create(destFile)


    		 reader = new BufferedReader(new InputStreamReader(in))


    		 writer = new BufferedWriter(new OutputStreamWriter(out))
         line = reader.readLine()

      	 while (  line != null ) {
        	   logDebug("[move] :"+line)
        	   writer.newLine()
        		 writer.write(line)
        		 line = reader.readLine()

      	 }
      	 writer.flush();

      	 //删除文件
    	   fs.delete(new Path(srcFileName), true);

    	   true
    	 }
    	 finally
    	 {
      	 reader.close();
      	 writer.close();
      	 in.close()
      	 out.close()

      	 fs.close();
    	 }

   }
     /**
      *
     * 函数名    : moveFilesToPath
     * 功   能    :  在HDFS目录下的文件移动到其他目录下
     * 参   数    : @param conf
     * 参   数    : @param srcPath
     * 参   数    : @param destPath
     * 参   数    : @return
     * 参   数    : @throws IOException    设定文件
     * 返回值	 : boolean    返回类型
     * 编写者    : root
     * 编写时间 : 2016年11月7日 下午4:09:26
      */
  def moveFilesToPath( conf : Configuration,  srcPath : String,  destPath : String) : Unit = {
     val fs : FileSystem =  FileSystem.get(conf)
     try
     {
         if(!fs.exists(new Path(srcPath)) || !fs.exists(new Path(destPath))){
           logInfo("[moveFilesToPath] : ("+srcPath+" OR "+destPath+") is not exist!")
           return
         }
         if(fs.isFile(new Path(srcPath)) || fs.isFile(new Path(destPath))){
            logError("[moveFilesToPath] : ("+srcPath+" OR "+destPath+") must be directory!")
            throw new IllegalArgumentException(" ("+srcPath+" OR "+destPath+") must be directory!")
         }

         val files = fs.listStatus(new Path(srcPath)).filter { _.isFile() }
         for(file <- files) {
               val srcFile = srcPath+"/"+file.getPath.getName
               val destFile = destPath+"/"+file.getPath.getName
               logInfo("moveFilesToPath :"+srcFile+" to "+destFile+" is ok!")
               move(conf,srcFile,destFile)

         }

     }

     finally
     {
         if(fs != null) fs.close()

     }
  }


  def delete( conf : Configuration,  delPath : String, recursive : Boolean) : Boolean = {
     val fs : FileSystem =  FileSystem.get(conf)
     try
     {
       fs.delete(new Path(delPath), recursive)
     }
     finally
     {
         if(fs != null) fs.close()

     }
  }

    /**
     * 函数名    : delete
     * 功   能    : 删除文件
     * 参   数    : @param conf
     * 参   数    : @param delPath

     * 返回值	 : Boolean
     * 编写者    : root
     * 编写时间 : 2017年02月09日 下午15:38:26
     */
   def delete( conf : Configuration,  delPath : String) : Boolean = {
     val fs : FileSystem =  FileSystem.get(conf)
     try
     {

        val p = new Path(delPath)
        if( fs.exists(p) ) {
           logInfo("delete: "+delPath+" is  exists!")
          fs.delete(p, true)
        }
        else {
          logInfo("delete: "+delPath+" is not  exists!")
          true
        }
     }
     finally
     {
         if(fs != null) fs.close()

     }
  }

       /**
      * 创建目录
      *
      * @param conf
      * @param dirName
      * @return
      * @throws IOException
      */
     def createDirectory(conf : Configuration , dirName : String )  : Boolean =  {
         val fs = FileSystem.get(conf);
         try
         {
           val dir = new Path(dirName);
           if(!fs.exists(dir)) {
             //logInfo("no exist")
             fs.mkdirs(dir)

           }
           else {
             //logInfo(" exist")
             false
           }
         }
         finally
         {
           fs.close();
         }

     }
      /**
      * 列出指定目录下的文件\子目录信息（非递归）
      *
      * @param conf
      * @param dirPath
      * @return
      * @throws IOException
      */
     def  listStatus(conf : Configuration , dirPath : String)  : Array[FileStatus] = {
         var files : Array[FileStatus] = null
         val fs = FileSystem.get(conf);
         try
         {
           files = fs.listStatus(new Path(dirPath))


         }
         catch{
           case ioe: IOException =>  null
         }
         finally
         {
           fs.close()
         }
         if(files == null ) null
         else files
     }
  /**
      * 列出指定目录下的文件\子目录信息（递归）
      *
      * @param conf
      * @param dirPath
      * @return
      * @throws IOException
      */
     def  listAllStatus(conf : Configuration , dirPath : String)  : Array[FileStatus] = {

         var result = new ArrayBuffer[FileStatus]()

         val fs = FileSystem.get(conf);
         try
         {
           val files = fs.listStatus(new Path(dirPath))
           files.foreach { x =>
               if(x.isDirectory()) {
                   val childPath = dirPath + "/" + x.getPath.getName
                   log.info("listAllStatus :[childPath] :  "+childPath)
                   result ++= listAllStatus(conf,childPath)
               }
               else
                 result += x
           }
           result.toArray

         }
         catch{
           case ioe: IOException =>  null
         }
         finally
         {
           fs.close()
         }
     }

        /**
    * 判断是文件还是路径
    *
    * @param conf
    * @param path
    * @return int 0:file 1: dir
    * @throws IOException
    */
   def isDirOrFile(conf : Configuration ,  path : String) : Int = {
       val fs = FileSystem.get(conf);
       if(fs.isDirectory(new Path(path)))
    	    1
       else  if(fs.isFile(new Path(path)))
    	    0
       else
          -1
   }
     /**
     *
     * 函数名    : createFile
     * 功   能    : 创建文件,如果没有相应的文件夹，则创建。并删除已存在的文件
     * 参   数    : @param conf
     * 参   数    : @param path
     * 参   数    : @param fileName
     * 参   数    : @param fileContent
     * 参   数    : @return
     * 返回值	 : 无
     * 编写者    : root
     * 编写时间 : 2017年02月09日 下午15:38:26
      */
   def createFile(conf : Configuration ,  path : String, fileName : String,  fileContent : String) : Unit =  {
	   if(!HDFSUtil.exits(conf, path)) createDirectory(conf, path)
	   var filePath = path + "/" + fileName
	   delete(conf, filePath)

     createFile(conf, filePath, fileContent.getBytes())

   }
     /**
     *
     * 函数名    : createFile
     * 功   能    : 创建文件
     * 参   数    : @param conf
     * 参   数    : @param path
     * 参   数    : @param fileName
     * 参   数    : @param contents
     * 参   数    : @return
     * 返回值	 : 无
     * 编写者    : root
     * 编写时间 : 2017年02月09日 下午15:38:26
      */
    def createFile(conf : Configuration,filePath : String,contents : Array[Byte] ) : Unit = {
       val fs = FileSystem.get(conf)
       val path = new Path(filePath)
       val outputStream = fs.create(path)
       outputStream.write(contents)

       outputStream.close()
       fs.close()
   }

    /**
     * 函数名    : exits
     * 功   能    : 文件是否存在
     * 参   数    : @param conf
     * 参   数    : @param path
     * 返回值	 : Boolean
     * 编写者    : root
     * 编写时间 : 2017年02月09日 下午15:38:26
     */
   def  exits(conf : Configuration,  path : String) : Boolean =  {
       val fs = FileSystem.get(conf)
       fs.exists(new Path(path))

   }

     /**
     * 函数名    : put
     * 功   能    : 把本地文件放到HDFS系统的remoteFilePath目录上
     * 参   数    : @param conf
     * 参   数    : @param localFilePath
     * 参   数    : @param remoteFilePath
     * 返回值	 : 无
     * 编写者    : root
     * 编写时间 : 2017年02月09日 下午15:38:26
     */
   def  put(conf : Configuration,  localFilePath : String , remoteFilePath : String ) ={
     val fs = FileSystem.get(conf)
     val localPath = new Path(localFilePath)
     val remotePath = new Path(remoteFilePath)
     fs.copyFromLocalFile(false, true, localPath, remotePath)

     fs.close()
   }

     /**
     * 函数名    : put
     * 功   能    : 把HDFS系统的remoteFilePath目录上下放到本地目录
     * 参   数    : @param conf
     * 参   数    : @param remoteFilePath
     * 参   数    : @param localFilePath
     * 返回值	 : 无
     * 编写者    : root
     * 编写时间 : 2017年02月09日 下午15:38:26
     */
   def get(conf : Configuration,  remoteFilePath : String ,  localFilePath: String ) = {
       val fs = FileSystem.get(conf);
       val localPath = new Path(localFilePath);
       val remotePath = new Path(remoteFilePath);
       fs.copyToLocalFile( remotePath, localPath);

       fs.close();
   }

  /**
      * 读取文件内容
      *
      * @param conf
      * @param filePath
      * @return
      * @throws IOException
      */
     def  readFile(conf : Configuration, filePath : String )  : String =  {
         var fileContent : String = null
         val fs = FileSystem.get(conf)

         val path = new Path(filePath)
         var inputStream : InputStream = null
         var outputStream : ByteArrayOutputStream = null
         try {
             inputStream = fs.open(path)
             outputStream = new ByteArrayOutputStream(inputStream.available())
             IOUtils.copyBytes(inputStream, outputStream, conf)


             fileContent = outputStream.toString()
         } finally {
             IOUtils.closeStream(inputStream)
             IOUtils.closeStream(outputStream)
             fs.close();
         }
         return fileContent;
     }
     def exist(conf : Configuration, filePath : String ) : Boolean = {
         val fs = FileSystem.get(conf)

         val path = new Path(filePath)
         fs.exists(path)
     }
  /**
    *
    * 函数名    : moveFilesToPath
    * 功   能    :  在HDFS目录下的文件移动到其他目录下
    * 参   数    : @param conf
    * 参   数    : @param srcPath
    * 参   数    : @param destPath
    * 参   数    : @return
    * 参   数    : @throws IOException    设定文件
    * 返回值	 : boolean    返回类型
    * 编写者    : root
    * 编写时间 : 2016年11月7日 下午4:09:26
    */
  def moveFilesToPathExt( fs : FileSystem,  srcPath : String,  destPath : String) : Unit = {

    try
    {

      if(!fs.exists(new Path(srcPath)) ){
        logInfo("[moveFilesToPath] : ("+srcPath+" OR "+destPath+") is not exist!")
        return
      }
     /* if(fs.isFile(new Path(srcPath)) || fs.isFile(new Path(destPath))){
        logError("[moveFilesToPath] : ("+srcPath+" OR "+destPath+") must be directory!")
        throw new IllegalArgumentException(" ("+srcPath+" OR "+destPath+") must be directory!")
      }*/

      val files = fs.listStatus(new Path(srcPath))//.filter { _.isFile() }
      val fileCnt = files.length
      var i = fileCnt
      for(file <- files) {
        if(file.isFile) {
          if(file.getPath.getName.indexOf("part-") > -1) {
            val srcFile = srcPath + "/" + file.getPath.getName
            var destFile = destPath + "/" + file.getPath.getName + "-" + i + "-" + MathUtil.random(3)
            if (fs.exists(new Path(destFile))) {
              destFile = destPath + "/" + file.getPath.getName + "-" + i + "-r" + MathUtil.random(3)
            }
            logInfo("moveFilesToPathExt :" + srcFile + " to " + destFile + " is ok!")
            println("moveFilesToPathExt :" + srcFile + " to " + destFile + " is ok!")
            moveExt(fs, srcFile, destFile)
            i = i + 1
          }
        }
        else{
          val nextSrcPath = srcPath+"/"+file.getPath.getName
          val nextDestPath = destPath+"/"+file.getPath.getName
          moveFilesToPathExt(fs,nextSrcPath,nextDestPath)
        }

      }

    }

    finally
    {


    }
  }
  /**
    *
    * 函数名    : moveFilesToPathExtMain
    * 功   能    :  在HDFS目录下的文件移动到其他目录下
    * 参   数    : @param conf
    * 参   数    : @param srcPath
    * 参   数    : @param destPath
    * 参   数    : @return
    * 参   数    : @throws IOException    设定文件
    * 返回值	 : boolean    返回类型
    * 编写者    : root
    * 编写时间 : 2016年11月7日 下午4:09:26
    */
  def moveFilesToPathExtMain( conf : Configuration,  srcPath : String,  destPath : String) : Unit = {
    val fs : FileSystem =  FileSystem.get(conf)
    try
    {

      moveFilesToPathExt(fs,srcPath,destPath)

    }

    finally
    {
      if(fs != null) fs.close()

    }
  }
  /**
    *
    * 函数名    : moveExt
    * 功   能    :  在HDFS上移动文件(目的目录已存在，则追加文件)
    * 参   数    : @param conf
    * 参   数    : @param srcFileName
    * 参   数    : @param destFileName
    * 参   数    : @return
    * 参   数    : @throws IOException    设定文件
    * 返回值	 : boolean    返回类型
    * 编写者    : root
    * 编写时间 : 2016年11月7日 下午4:09:26
    */
  def moveExt( conf : Configuration,  srcFileName : String,  destFileName : String) :Boolean = {
    val fs = FileSystem.get(conf)
    val srcFile = new Path(srcFileName)
    val destFile = new Path(destFileName)
    //目标文件存在，则删除
    if( fs.exists(destFile) )
    {
      fs.delete(destFile, false)
    }
    val destFilePath = destFileName.substring(0, destFileName.lastIndexOf("/"))
    val destPath = new Path(destFilePath)
    //目标文件所在目录不存在，则创建
    if( !fs.exists(destPath) )
    {
      fs.mkdirs(destPath);
    }

    var in : InputStream = null
    var reader : BufferedReader = null
    var writer : BufferedWriter = null
    var out : OutputStream = null
    var line : String = null


    try
    {
      in = fs.open(srcFile)
      out = fs.create(destFile)


      reader = new BufferedReader(new InputStreamReader(in))


      writer = new BufferedWriter(new OutputStreamWriter(out))
      line = reader.readLine()

      while (  line != null ) {
        logDebug("[move] :"+line)
        writer.newLine()
        writer.write(line)
        line = reader.readLine()

      }
      writer.flush();

      //删除文件
      fs.delete(new Path(srcFileName), true);

      true
    }
    finally
    {
      reader.close();
      writer.close();
      in.close()
      out.close()

      fs.close();
    }

  }

  /**
    *
    * 函数名    : moveExt
    * 功   能    :  在HDFS上移动文件(目的目录已存在，则追加文件)
    * 参   数    : @param conf
    * 参   数    : @param srcFileName
    * 参   数    : @param destFileName
    * 参   数    : @return
    * 参   数    : @throws IOException    设定文件
    * 返回值	 : boolean    返回类型
    * 编写者    : root
    * 编写时间 : 2016年11月7日 下午4:09:26
    */
  def moveExt( fs: FileSystem,  srcFileName : String,  destFileName : String) :Boolean = {

    val srcFile = new Path(srcFileName)
    val destFile = new Path(destFileName)
    //目标文件存在，则删除
    if( fs.exists(destFile) )
    {
      fs.delete(destFile, false)
    }
    val destFilePath = destFileName.substring(0, destFileName.lastIndexOf("/"))
    val destPath = new Path(destFilePath)
    //目标文件所在目录不存在，则创建
    if( !fs.exists(destPath) )
    {
      fs.mkdirs(destPath);
    }

    var in : InputStream = null
    var reader : BufferedReader = null
    var writer : BufferedWriter = null
    var out : OutputStream = null
    var line : String = null


    try
    {
      in = fs.open(srcFile)
      out = fs.create(destFile)


      reader = new BufferedReader(new InputStreamReader(in))


      writer = new BufferedWriter(new OutputStreamWriter(out))
      line = reader.readLine()

      while (  line != null ) {
        logDebug("[move] :"+line)
        writer.newLine()
        writer.write(line)
        line = reader.readLine()

      }
      writer.flush();

      //删除文件
      fs.delete(new Path(srcFileName), true);

      true
    }
    finally
    {
      if(reader != null ) reader.close();
      if(writer != null ) writer.close();
      if(in != null ) in.close()
      if(out != null ) out.close()


    }

  }
}
