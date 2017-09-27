package com.secray.bdp.stat.sql

import com.secray.bdp.stat.mapduce.base.{Constant, SparkBase, SparkBaseInf}
import com.secray.bdp.utils.DateUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 2017/9/7 0007.
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
;
  */
class EsSql( statType : String,  statDayMon : String = DateUtil.formatDateStr(DateUtil.SHORTDAY, -1) )
  extends SparkBase(  statType, statDayMon )  with SparkBaseInf  {

  super.setSparkBaseInf(this)

  /**
    *
    * 函数名    : loadConf
    * 功   能   : 加载全局变量
    * 参   数   : @param  conf
    * 返回值    : void
    * 编写者    : root
    * 编写时间  : 2017-06-13 17:10:16 下午 5:10
    */
  def loadConf(conf : Configuration) : Unit = {

    logInfo("[loadConf] : "+conf.toString())

  }

  def executeSql() : Option[String] = {
    val sql="select * from tb_asmc"
    /*val year = this.statDayMon.substring(0,4)
    val month = this.statDayMon.substring(5,6)
    val day = this.statDayMon.substring(7,8)
    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val ss =  SparkSession.builder().getOrCreate()
    import ss.implicits._
    import ss.sql
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hisSql="ALTER TABLE tb_log_his DROP IF EXISTS PARTITION(year = "+ year +", month = "+month+", day = "+day+")"
    hisSql = "insert into tb_log_his \n"+
             "select operatecndition ,inserttime ,  numid,businesssys_registeredid, terminalid ,type ,userid\n"+
             ",operatename,organizationid  ,operateresult,timestamps ,operatetime ,organization   ,versions\n,"+
             "id,operatetype  ,responepackage  ,errorcode,username  ,\n"+
             "year(operatetime) as year ,month(operatetime) as month, day(operatetime) as day\n"+
            "from tb_log limit "*/
    this.buildTempView(None,None,Some("tb_asmc"),None)
    Some(sql)
  }

}
object EsSql{
  def main(args: Array[String]): Unit = {
     if( args.length == 3 ) {

       val esSql = new EsSql(Constant.STAT,args(2))
       esSql.setInOutPath(args(0),args(1))

       esSql.run()
     }
     else  if( args.length == 2 ) {

       val esSql = new EsSql(Constant.STAT)
       esSql.setInOutPath(args(0),args(1))
       esSql.run()
     }
     else  if( args.length == 0 ) {

       val esSql = new EsSql(Constant.STAT)
       esSql.run()
     }
    else{
       println("[error] ： 参数不对！")
       println("参数格式：输入目录 输出目录 统计日期")
       println("例子：runStat.sh es:bank/account es:/stat/bank 20170917")
       println("例子：runStat.sh es:bank/account es:/stat/bank")
     }


  }
}

