package  com.secray.bdp.utils

import com.secray.bdp.stat.mapduce.base.Constant

import scala.xml.XML
import scala.xml.NodeSeq



object XmlUtil {
   // println(XmlUtil.getClass.getResource("/").getPath)
  println(getConfPath("./conf/threshold.xml"))
    val xmlFile =XML.loadFile (getConfPath("./conf/threshold.xml"))

    def getConfPath(confDir : String ) : String = {
      var confPath = confDir
       if(SysUtil.isWin()){
            confPath = System.getProperty("user.dir") + SysUtil.getFileSeparator() + "asmcbdp" + SysUtil.getFileSeparator() +confPath

       }
      //println(confPath)
      confPath

    }
    def getThresholdByName(thresholdName : String) : Option[String] = {
       val propertys  = xmlFile \\ "configuration" \\ "property"


       for( property <- propertys){
           //println("getThresholdByName : "+ (property \\ "name").text +" : " +  (property \\ "value").text )
           if(thresholdName.trim().equals( (property \\ "name").text.trim())){
              return Some((property \\ "value").text.trim())
           }

       }



       return None
    }
  def main(args: Array[String]): Unit = {

    println(XmlUtil.getThresholdByName("stat.inputOutPath"))
  }

}
