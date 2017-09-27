package  com.secray.bdp.utils

import java.util.{ Properties}

import org.apache.log4j.PropertyConfigurator
import org.slf4j.Logger
import org.apache.spark.util.SignalUtils
import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.util.ThreadStackTrace




private[secray] object Utils {



  /**
   * Get the ClassLoader which loaded Spark.
   */
  def getBdpClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getBdpClassLoader)



  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }



  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef): String = {
    obj.getClass.getSimpleName.replace("$", "")
  }





  /**
   * configure a new log4j level
   */
  def setLogLevel(l: org.apache.log4j.Level) {
    org.apache.log4j.Logger.getRootLogger().setLevel(l)
  }

  /**
   * config a log4j properties used for testsuite
   */
  def configTestLog4j(level: String): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    PropertyConfigurator.configure(pro)
  }






}
