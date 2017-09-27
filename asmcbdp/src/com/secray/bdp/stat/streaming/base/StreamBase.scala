package  com.secray.bdp.stat.streaming.base

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import com.secray.bdp.log.Logging
import com.secray.bdp.utils.CommonUtil
import com.secray.bdp.utils.XmlUtil

import kafka.serializer.StringDecoder



abstract class StreamBase( statType : String,   group : String, topics : String ,numThreads : String) extends Serializable with Logging {

  protected val jarLocation = XmlUtil.getThresholdByName("jarLocation").getOrElse("hdfs://192.168.12.9:8020/apps/spark/spAsmc.jar")
	protected val jarGenPath = XmlUtil.getThresholdByName("jarGenPath").getOrElse("d:\\yf\\ljr\\spG01.jar")

	protected val warehouseLocation = XmlUtil.getThresholdByName("warehouseLocation").getOrElse("hdfs://192.168.12.9:8020/apps/spark/warehouse")
	protected val sparkAddress = XmlUtil.getThresholdByName("sparkAddress").getOrElse("spark://192.168.12.9:17077")
	protected val jobTrackerAddress = XmlUtil.getThresholdByName("jobTrackerAddress").getOrElse("192.168.12.9:50090")
	protected val hdfsAddress = XmlUtil.getThresholdByName("hdfsAddress").getOrElse("hdfs://192.168.12.9:8020")
	protected val user = XmlUtil.getThresholdByName("user").getOrElse("root")

  protected val iSeparator = XmlUtil.getThresholdByName("inputSeparator").getOrElse(",")
  protected val escSep =  CommonUtil.addEscapeCHar(iSeparator)

  protected val kfHosts = XmlUtil.getThresholdByName("kfHosts").getOrElse("master.xdbd:19092")
  protected val readDataInterval = XmlUtil.getThresholdByName("readDataInterval").getOrElse("5")



  protected var ssc : StreamingContext =_
  protected var topicsMap : Set[String] =_
  protected var kafkaParams : Map[String,String] =_
  protected var streamBaseInf : StreamBaseInf = _

  def setSparkBaseInf(streamBaseInf : StreamBaseInf) = {
	     this.streamBaseInf = streamBaseInf
	}

  /**
    *
    * 函数名    : initConfig
    * 功   能   : 初始化stream程序的配置
    * 参   数   : @param
    * 返回值    : void
    * 编写者    : root
    * 编写时间  : 2017-06-07 15:49:20 下午 3:49
    */
  def initConfig() : Unit  = {

	       val sparkConf = new SparkConf().setAppName(statType).setMaster(this.sparkAddress)
         this.ssc = new StreamingContext(sparkConf, Seconds(readDataInterval.toInt))
	       ssc.checkpoint("checkpoint")
	       topicsMap = topics.split(",").toSet


         kafkaParams = Map[String, String](
              "metadata.broker.list" -> kfHosts,
              "serializer.class" -> "kafka.serializer.StringEncoder")

	}
  /**
    *
    * 函数名    : execute
    * 功   能   : 执行子类的处理数据流方法
    * 参   数   : @param
    * 返回值    : void
    * 编写者    : root
    * 编写时间  : 2017-06-07 15:36:21 下午 3:36
    */
  def execute() : Unit = {

       val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsMap)
       val lines = messages.map(_._2)
       this.streamBaseInf.handleData(lines, statType, escSep, iSeparator)
       ssc.start()
       ssc.awaitTermination()
  }

  def run() : Unit = {
      this.initConfig()

      this.execute()

  }

}