package  com.secray.bdp.utils

object SysUtil {

  def getEnv(env : String) : String ={
    System.getenv(env)
  }

  def getProperty(property : String) : String ={
    System.getProperty(property)
  }


  def getJavaHome() : String  = {
    getEnv("JAVA_HOME")
  }

  def getScalaHome() : String  = {
     getEnv("SCALA_HOME")
  }

  def getOs() : String = {
      getProperty("os.name");
  }

  def getPathSeparator() : String = {
      getProperty("path.separator");
  }

  def getFileSeparator() : String = {
      getProperty("file.separator");
  }



  def isWin() : Boolean = {
     val os = getOs()
     if (os != null && os.startsWith("Windows")) true
     else false
  }
  def isLinux() : Boolean = {
     val os = getOs()
     if (os != null && os.equals("Linux")) true
     else false
  }
  def getClassPathSeperator() : String = {
      val os = getOs()
      if (os != null ) {
         if( os.equals("Linux") ) ":"
         else if(os.startsWith("Windows")) ";"
         else null
      }
      else null
  }

  def main(args: Array[String]): Unit = {
       println(SysUtil.getPathSeparator()+" -> "+SysUtil.getFileSeparator())
   }
}
