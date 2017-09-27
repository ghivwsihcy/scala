package com.secray.bdp.stat.sql

import com.secray.bdp.stat.mapduce.base.Constant
import com.secray.bdp.utils.{DateUtil, XmlUtil}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
CREATE EXTERNAL TABLE tb_log  (
operatecndition          STRING,
inserttime               timestamp,
numid                    STRING,
businesssys_registeredid STRING,
terminalid               STRING,
type                     STRING,
userid                   STRING,
operatename              STRING,
organizationid           STRING,
operateresult            bigint,
timestamps               timestamp,
operatetime              TIMESTAMP,
organization             STRING,
versions                 bigint,
id                       bigint,
operatetype              bigint,
responepackage           bigint,
errorcode                bigint,
  username string
)

  STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
  TBLPROPERTIES('es.resource' = 'asmc/businessosyslog','es.index.auto.create' = 'true', 'es.field.read.empty.as.null' = 'true','es.nodes' = '192.168.12.35','es.port' = '9200');

  hive -hiveconf hive.aux.jars.path=/opt/cloudera/parcels/CDH-5.9.3-1.cdh5.9.3.p0.4/lib/hive/auxlib/elasticsearch-hadoop-5.5.2.jar

  select
operatecndition ,inserttime ,  numid,businesssys_registeredid, terminalid ,type ,userid
,operatename,organizationid  ,operateresult,timestamps ,operatetime ,organization   ,versions
,id,operatetype  ,responepackage  ,errorcode,username
from tb_log limit 2;

  CREATE TABLE tb_log_his (
 operatecndition          STRING,
inserttime               timestamp,
numid                    STRING,
businesssys_registeredid STRING,
terminalid               STRING,
type                     STRING,
userid                   STRING,
operatename              STRING,
organizationid           STRING,
operateresult            bigint,
timestamps               timestamp,
operatetime              TIMESTAMP,
organization             STRING,
versions                 bigint,
id                       bigint,
operatetype              bigint,
responepackage           bigint,
errorcode                bigint,
  username string

)
PARTITIONED BY (year int,month int, day int)

  set hive.exec.dynamic.partition.mode=nonstrict;

  insert into tb_log_his PARTITION (year,month,day)
select
operatecndition ,inserttime ,  numid,businesssys_registeredid, terminalid ,type ,userid
,operatename,organizationid  ,operateresult,timestamps ,operatetime ,organization   ,versions
,id,operatetype  ,responepackage  ,errorcode,username  ,year(operatetime) as year ,month(operatetime) as month, day(operatetime) as day
from tb_log limit 2;

  */
class HiveSql( statType : String, statDayMon : String = DateUtil.formatDateStr(DateUtil.SHORTDAY, -1) )
{
  protected val warehouseLocation = XmlUtil.getThresholdByName("warehouseLocation").getOrElse("hdfs://192.168.12.9:8020/apps/spark/warehouse")
  protected val sparkAddress = XmlUtil.getThresholdByName("sparkAddress").getOrElse("spark://192.168.12.9:17077")
  protected val hdfsAddress = XmlUtil.getThresholdByName("hdfsAddress").getOrElse("hdfs://192.168.12.9:8020")
  protected val executeMemory = XmlUtil.getThresholdByName("executeMemory").getOrElse("1g")
  protected val esNodes = XmlUtil.getThresholdByName("esNodes").getOrElse("192.168.12.9")
  protected val esPort = XmlUtil.getThresholdByName("esPort").getOrElse("9200")

  protected var inEsPath: String=""
  protected  var outEsPath : String=""

  def setInOutPath( inEsPath: String,outEsPath : String ) :Unit   = {
      this.inEsPath = inEsPath
     this.outEsPath = outEsPath

  }
  def buildTempView(spark :SparkSession,tableName : Option[String] = None,fileName : Option[String])  = {
    var descPath = ""
    if (!fileName.isEmpty) {
      descPath = fileName.get
    }
    else {
      descPath = this.inEsPath

    }
    println("[buildTempView] :" +descPath)
      val sparkDF = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load(descPath)
      sparkDF.createOrReplaceTempView(tableName.getOrElse("bdpTb"))
  }
  def run()  = {

    val spark = SparkSession
      .builder
      .master(this.sparkAddress)
      .config("spark.sql.warehouse.dir", this.warehouseLocation)
      .config("spark.files.overwrite", true)
      .config("spark.executor.memory", executeMemory)
      .config("es.internal.es.version", "5.5.2")
      .config("es.index.auto.create", "true")
      .config("es.index.read.missing.as.empty", "true")
      .config("pushdown", "true")
      .config("es.nodes", this.esNodes)
      .config("es.port", this.esPort)
      .config("es.nodes.wan.only", "true")
      .enableHiveSupport()
      .appName(statType)
      .getOrCreate()

    var v_sql=""

    val year = this.statDayMon.substring(0,4)
    val month = this.statDayMon.substring(4,6)
    val day = this.statDayMon.substring(6,8)



    //在hive中插入数据到历史表
    import spark.sql
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    println("[hiveSql:0] : "+"set hive.exec.dynamic.partition.mode=nonstrict")
    v_sql = "ALTER TABLE tb_log_his DROP IF EXISTS PARTITION(year = "+ year +", month = "+Integer.parseInt(month)+", day = "+Integer.parseInt(day)+")"
    println("[hiveSql:1] : "+v_sql)
    sql(v_sql)
    //把当前数据插入历史表
    v_sql = "insert into tb_log_his PARTITION (year,month,day) \n"+
             "select operatecndition ,inserttime ,  numid,businesssys_registeredid, terminalid ,type ,userid\n "+
             ",operatename,organizationid  ,operateresult,timestamps ,operatetime ,organization   ,versions,\n "+
             "id,operatetype  ,responepackage  ,errorcode,username  ,\n "+
             "year(operatetime) as year ,month(operatetime) as month, day(operatetime) as day\n "+
            "from tb_log  limit 100"
    println("[hiveSql:2] : "+v_sql)
    sql(v_sql)


    //注册spark sql的表
    buildTempView(spark,Some("tb_log"),None)
    v_sql="select * from tb_log where operatetime='"+statDayMon+"' limit 2"
    println("[hiveSql:stat] : "+v_sql)
    //保存数据到ES
    val sqlDf = spark.sql(v_sql)
    EsSparkSQL.saveToEs(sqlDf, this.outEsPath+statDayMon)


  }

}
object HiveSql{
  def main(args: Array[String]): Unit = {
     if( args.length == 3 ) {

       val hiveSql = new HiveSql(Constant.STAT,args(2))
       hiveSql.setInOutPath(args(0),args(1))

       hiveSql.run()
     }
     else  if( args.length == 2 ) {

       val hiveSql = new HiveSql(Constant.STAT)
       hiveSql.setInOutPath(args(0),args(1))
       hiveSql.run()
     }

    else{
       println("[error] ： 参数不对！")
       println("参数格式：输入目录 输出目录 统计日期")
       println("例子：runStat.sh es:bank/account es:/stat/bank 20170917")
       println("例子：runStat.sh es:bank/account es:/stat/bank")
     }


  }
}

