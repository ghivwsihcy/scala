package  com.secray.bdp.stat.mapduce.base

import org.apache.hadoop.conf.Configuration

import com.secray.bdp.utils.LogUtil



trait SparkBaseInf extends java.io.Serializable{

  def loadConf(conf : Configuration)



  def executeSql() : Option[String]
}
