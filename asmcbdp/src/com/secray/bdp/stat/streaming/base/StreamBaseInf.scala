package  com.secray.bdp.stat.streaming.base

import org.apache.spark.streaming.dstream.DStream

trait StreamBaseInf {
   def handleData(lines : DStream[String],statType : String,escSep : String,seperator :String)
}