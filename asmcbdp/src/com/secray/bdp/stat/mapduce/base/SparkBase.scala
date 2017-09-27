package  com.secray.bdp.stat.mapduce.base

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import com.secray.bdp.log.Logging
import com.secray.bdp.utils._
import java.io.File
import java.util.Properties
import org.elasticsearch.spark.sql._






abstract class SparkBase(  statType : String,  statDayMon : String) extends Serializable with Logging {



	protected val jarLocation = XmlUtil.getThresholdByName("jarLocation").getOrElse("hdfs://192.168.12.9:8020/apps/spark/spAsmc.jar")
	protected val jarGenPath = XmlUtil.getThresholdByName("jarGenPath").getOrElse("d:\\yf\\ljr\\spG01.jar")

	protected val warehouseLocation = XmlUtil.getThresholdByName("warehouseLocation").getOrElse("hdfs://192.168.12.9:8020/apps/spark/warehouse")
	protected val sparkAddress = XmlUtil.getThresholdByName("sparkAddress").getOrElse("spark://192.168.12.9:17077")
  protected val hdfsAddress = XmlUtil.getThresholdByName("hdfsAddress").getOrElse("hdfs://192.168.12.9:8020")
	protected val esNodes = XmlUtil.getThresholdByName("esNodes").getOrElse("192.168.12.9")
	protected val esPort = XmlUtil.getThresholdByName("esPort").getOrElse("9200")
	protected val user = XmlUtil.getThresholdByName("user").getOrElse("root")
  protected val executeMemory = XmlUtil.getThresholdByName("executeMemory").getOrElse("1g")

  protected var iSeparator = XmlUtil.getThresholdByName("inputSeparator").getOrElse(",")
  def getSeparator = iSeparator
  protected var escSep =  CommonUtil.addEscapeCHar(iSeparator)
  protected var inputOutPath = XmlUtil.getThresholdByName(statType+".inputOutPath").get
  def getInputOutPath = inputOutPath


  protected var inputPath : Option[String] = None
  protected var outputPath : Option[String] = None
  protected var outputSrcPath : Option[String] = None

  //@transient
	protected var sparkBaseInf : SparkBaseInf = null
	@transient
	protected var conf : Configuration = _
	//@transient
	protected var spark : SparkSession = _

  //目录深度
  protected var pathDepth : Int = 1;
  protected var inputPathArg = ""
  protected var outputPathArg = ""



  def getConfig( configName : String,defaultValue : String ) : String = {
     XmlUtil.getThresholdByName(configName).getOrElse(defaultValue)
  }

  def getConf( ) : Configuration = {
    this.conf
  }
  def setPathDepth(pathDepth : Int) = {

    this.pathDepth = pathDepth
  }


  def setInOutPath( inputPathArg : String,outputPathArg : String ) :Unit   = {
    if(inputPathArg != null && inputPathArg.trim.length > 0 ) {
      this.inputOutPath = inputPathArg + ","+ inputOutPath.split(",")(1)
    }
    if(outputPathArg != null && outputPathArg.trim.length > 0 ){
      this.outputPathArg = inputOutPath.split(",")(0) +""+outputPathArg
    }

  }

	def setSparkBaseInf(sparkBaseInf : SparkBaseInf) = {
	     this.sparkBaseInf = sparkBaseInf
	}
  /**
    *
    * 函数名    : initConfig
    * 功   能   : 初始化程序的配置及运行环境
    * 参   数   : @param
    * 返回值    : void
    * 编写者    : root
    * 编写时间  : 2017-06-07 17:30:38 下午 5:30
    */
	def initConfig() : Unit  = {


          this.spark = SparkSession
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
            //.config("spark.yarn.jar", this.hdfsAddress+"/apps/spark/spark-assembly-1.6.0-cdh5.9.3-hadoop2.6.0-cdh5.9.3.jar")
            .appName(statType)
            .getOrCreate()


    //加载依赖的第三方jar包
        jarLocation.split(",").map{ x =>
           // logInfo("initConfig -> 分发jar  "+ x)
            println("initConfig -> 分发jar  "+ x)
            spark.sparkContext.addJar(x)
        }

     /*   var i = -1
        if( SysUtil.isWin() ) {
           jarLocation.split(",").map{ x =>
             logInfo("initConfig -> 分发jar  "+ x)
             spark.sparkContext.addJar(x)
           }
        }
        else
        {
          jarLocation.split(",").map{x =>
             i = i + 1
             (i,x)
          }.filter(_._1 == 0 ).map( x => spark.sparkContext.addJar(x._2) )
        }*/

        this.conf  = this.spark.sparkContext.hadoopConfiguration
	     // logInfo("[initConfig] : Init conf")
	      this.sparkBaseInf.loadConf(conf)

        if(iSeparator != null && iSeparator.trim().length() > 0 ){
            conf.set("mapred.textoutputformat.ignoreseparator","true");
  	        conf.set("mapreduce.output.textoutputformat.separator",iSeparator);
          }
	}

   /**
   * 函数名    : initParameters
   * 功   能    : 初始化输入和输出目录(删除输出目录)
   * 参   数    : @param sql
   * 参   数    : @param schemaField  : 临时表的字段
   * 参   数    : @param tableName    : 表名
   * 参   数    : @param fileName     : 数据文件名
   * 返回值	 : 无
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
	def initParameters() : Unit = {
	   //获取配置文件中的输入和输出目录
	   var inputPathWithExpr = inputOutPath.split(",")(0)
     var outputPathWithExpr = inputOutPath.split(",")(1)

     if(inputPathWithExpr.toLowerCase().indexOf("es:") > -1 ){
        inputPathWithExpr = inputPathWithExpr.replace("es:","").replace("ES:","")
       this.inputPath = Some(BIVarUtil.formatVar(inputPathWithExpr,this.statDayMon).get)
     }
     else  {
       this.inputPath = Some(this.hdfsAddress+BIVarUtil.formatVar(inputPathWithExpr,this.statDayMon).get)
     }
     if(outputPathWithExpr.toLowerCase().indexOf("es:") > -1 ) {
       outputPathWithExpr = outputPathWithExpr.replace("es:","").replace("ES:","")
       this.outputPath = Some(outputPathWithExpr)
     }
     else {
       if(outputPathWithExpr.trim.length > 0 ) {
           this.outputPath = Some(this.hdfsAddress+outputPathWithExpr+BIVarUtil.getFormatYMDPath(this.statType, this.statDayMon,this.inputOutPath))
         //删除目标目录
         HDFSUtil.delete(this.conf, this.outputPath.getOrElse("NoPath"))
       }
       else  this.outputPath = Some("")
     }

	   logInfo("[initParameters] : "+ this.inputPath.get +" : " + this.outputPath.get)

	}



	/**
   * 函数名    : execute
   * 功   能    : 根据计算类型来进行mapreduce或者sql运算
   * 参   数    : @param sql
   * 参   数    : @param schemaField  : 临时表的字段
   * 参   数    : @param tableName    : 表名
   * 参   数    : @param fileName     : 数据文件名
   * 返回值	 : 无
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
	def execute() : Unit = {
        val sql = this.sparkBaseInf.executeSql()



          if (sql.getOrElse("").trim().length > 0) {
            val sqlDf = spark.sql(sql.get)




            //result.collect().foreach { x => println("sql data: "+x) }
            val outputFile = this.outputPath.get
            if (outputFile.trim.length > 0) {
              if (outputFile.toLowerCase().indexOf("hdfs:") > -1) {
                val result = sqlDf.rdd.map { x => x.mkString(this.iSeparator) }
                result.saveAsTextFile(this.outputPath.get)
              }
              else {

                EsSparkSQL.saveToEs(sqlDf, this.outputPath.get)


              }
            }
          }

          spark.stop()
	}

	 /**
   * 函数名    : buildTempView
   * 功   能    : 根据sql或者指定的字段创建spark临时表
   * 参   数    : @param sql
   * 参   数    : @param schemaField  : 临时表的字段
   * 参   数    : @param tableName    : 表名
   * 参   数    : @param fileName     : 数据文件名
   * 返回值	 : 无
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
	def buildTempView(sql : Option[String] = None,schemaField : Option[String] = None,tableName : Option[String] = None,fileName : Option[String])  = {

    if(this.outputPath.get.toLowerCase().indexOf("hdfs:") > -1 ) {
        //首先使用设置的字段信息，如果为空，则从sql中自动获取字段信息
        val schemaFieldStr = schemaField match {
          case Some(sf) => schemaField.get
          case None => StringUtil.getFields(sql).get
        }
        //logInfo("buildTempView -> " + schemaFieldStr)
        val schemaFields = schemaFieldStr.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(schemaFields)
        //如果文件目录为空，则取配置文件中的输入目录
        //var fName = this.inputPath.get
        var descPath = ""
        if (!fileName.isEmpty) {
            this.inputPath = fileName
            if(fileName.get.indexOf("*") == -1 )  descPath = getLoadFileNames().get
            else descPath = fileName.get
        }
        else {
          descPath = this.inputPath .get

        }
        //val data = spark.read.textFile(getLoadFiles().get).rdd
        LogUtil.log("【buildTempView】 :  " ,descPath);

        val data = spark.read.textFile(descPath).rdd.filter(_.length() > 0)
        val dfLog = data.map(_.split(escSep)).map(values=> Row(values:_*))


        val logDf = spark.createDataFrame(dfLog, schema)

        //logInfo("buildTempView -> "+this.inputPath.get+" : "+tableName.getOrElse("bdpTb"))
        LogUtil.log("【buildTempView】 :  " ,descPath +" : " + tableName.getOrElse("bdpTb"));
        logDf.createOrReplaceTempView(tableName.getOrElse("bdpTb"))
      }
      else{
        var descPath = ""
        if (!fileName.isEmpty) {
           descPath = fileName.get
        }
        else {
          descPath = this.inputPath .get

        }
        val sparkDF = spark.sqlContext.read.format("org.elasticsearch.spark.sql").load(descPath)
        sparkDF.createOrReplaceTempView(tableName.getOrElse("bdpTb"))
        sparkDF.collect()

      }
          //logInfo("buildTempView -> "+escSep)




	}

  /**
    * 函数名    : buildTableFromRdd
    * 功   能    : 根据sql或者指定的字段创建spark临时表(已rdd为数据源)
    * 参   数    : @param sql
    * 参   数    : @param schemaField  : 临时表的字段
    * 参   数    : @param tableName    : 表名
    * 参   数    : @param fileName     : 数据文件名
    * 返回值	 : 无
    * 编写者    : root
    * 编写时间 : 2017年03月09日 下午15:38:26
    */
  def buildTableFromRdd(sql : Option[String] = None,schemaField : Option[String] = None,tableName : Option[String] = None,dfLog: RDD[Row])  = {


    //首先使用设置的字段信息，如果为空，则从sql中自动获取字段信息
    val schemaFieldStr = schemaField match {
      case Some(sf) => schemaField.get
      case None => StringUtil.getFields(sql).get
    }
    logInfo("buildTableFromRdd -> "+schemaFieldStr)
    val schemaFields = schemaFieldStr.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(schemaFields)


    val logDf = spark.createDataFrame(dfLog, schema)
    logInfo("buildTableFromRdd -> "+this.inputPath.get+" : "+tableName.getOrElse("bdpTb"))
    logDf.createOrReplaceTempView(tableName.getOrElse("bdpTb"))
  }
	 /**
   * 函数名    : getLoadFileNames
   * 功   能    : 装载目录下的子目录下的文件
   * 返回值	 : 无
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
	def getLoadFileNames() :  Option[String]  = {
	   if( this.statType.toUpperCase().indexOf("MON") > -1  ) {
	     this.inputPath = Some(this.inputPath.get+"/*/*")
	   }
    else {
         var depthPath = this.inputPath.get
         for(i <- 1 to this.pathDepth) {
            depthPath += "/*"
         }
       this.inputPath = Some(depthPath)
     }
     this.inputPath
	}

	 /**
   * 函数名    : checkInputPath
   * 功   能    : 检查输入目录是否存在，是否是目录
   * 返回值	 : 合法: true 非法: false
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
	def checkInputPath() :  Boolean  = {
	    if(this.inputPath.isDefined) {
	      //目录是否存在
  	    if( HDFSUtil.exist(this.conf,this.inputPath.get) ){
  	      //如果是文件,返回false,是目录，返回true
  	      if(HDFSUtil.isDirOrFile(this.conf,this.inputPath.get) == 1 ) true
  	      else false

  	    }
  	    else false
	    }
	    else false

	}
	/**
	 * 暂时废弃
	 */
	def getLoadFiles() :  Option[String]  = {
	      //如果是文件
	      if(HDFSUtil.isDirOrFile(this.conf,this.inputPath.get) == 0 ) return  this.inputPath

	      val paths = HDFSUtil.listStatus(this.conf,this.inputPath.get)
	      if( paths !=null && !paths.isEmpty) {
	        logInfo("getLoadFiles -> ["+this.inputPath.get + "] is  【exist】!")
  	      import com.secray.bdp.implicits.ArrayFileImplicit._
  	      paths.convertToString("success.ok")
	      }
	      else {
	        logInfo("getLoadFiles -> ["+this.inputPath.get + "] is 【not】 exist!")
	        None
	      }

	}

	 /**
   * 函数名    : moveFilesToOfficalPath
   * 功   能    : 移动临时目录中的文件到正式目录
   * 返回值	 : 无
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
	def moveFilesToOfficalPath() : Unit ={
	   val OffPath = this.outputPath.get.replace("/tmp", "")
	   HDFSUtil.moveFilesToPath(this.conf,this.outputPath.get,OffPath)
	}
 /**
   * 函数名    : uploadJar
   * 功   能    : 把开发环境下的class打成jar包，并放到HFDS上(只有window下该功能才触发)
   * 返回值	 : 无
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
 def uploadJar() = {
   if( SysUtil.isWin() ) {
      //当前项目打包，上传到hdfs上
      ShellUtil.genUploadJar(this.jarGenPath,this.jarLocation.split(",")(0))
      //上传依赖的第三方jar包到hdfs上
      var i = -1
      val jarPath = XmlUtil.getConfPath("/lib/")

      jarLocation.split(",").map{x =>
         i = i + 1
         (i,x)
      }.filter(_._1 != 0 ).map{ x =>

          val fileName = x._2.substring(x._2.lastIndexOf("/")+1)
          val file = new File(jarPath+fileName)
          val conf = new Configuration()
          //logInfo("uploadJar -> "+file.getAbsolutePath+" : "+file.getName +" : "+ file.getPath)
          logInfo("uploadJar -> 本地目录【"+ file.getAbsolutePath + "] -> 【" + x._2 + "】"  )
          HDFSUtil.put(conf, file.getAbsolutePath, x._2)

      }
   }

 }
  /*
  CREATE TABLE tbc_md_attack_hacker_list(

    hack_id VARCHAR(30) NOT NULL,
    crate_date VARCHAR(30) NOT NULL,
    event_id VARCHAR(30) NOT NULL,
    url VARCHAR(300) NOT NULL,
    source_ip  VARCHAR(30) NOT NULL,

    PRIMARY KEY (`event_id`)
  )
  */
  /**
    *
    * 函数名    : importData
    * 功   能   : 把黑客合并的结果导入数据库表中
    * 参   数   : @param  hdfsTmpPath
    * 返回值    : void
    * 编写者    : root
    * 编写时间  : 2017-07-10 20:40:48 下午 8:40
    */
 def importData(hdfsTmpPath : String) = {

       if(hdfsTmpPath != null && hdfsTmpPath.trim.length > 0 ){
         val  url = XmlUtil.getThresholdByName("jdbcUrl").getOrElse("jdbc:mysql://192.168.12.12:3306/G01?useUnicode=true&characterEncoding=utf-8")
         val  user = XmlUtil.getThresholdByName("jdbcUser").getOrElse("root")
         val  pwd = XmlUtil.getThresholdByName("jdbcPwd").getOrElse("123456")

         var schemaField = "hack_id,event_id,url,source_ip,dept_name,site_domain,indu_name,attack_type,attack_time,status"
         //生成事件历史表
         buildTempView(None,Some(schemaField),Some("hacker"),Some(hdfsTmpPath+"/*"))
         val curDate = DateUtil.formatCurDateStr(DateUtil.LONGDAY)
         val sql="select hack_id,'"+curDate+"' as create_date,event_id,url,source_ip,dept_name,site_domain,indu_name,attack_type,attack_time,status from hacker"
         val hackerDf = spark.sql(sql)
         val properties=new Properties()
         properties.setProperty("user",user)
         properties.setProperty("password",pwd)
         hackerDf.write.mode(SaveMode.Append).jdbc(url,"tbc_md_attack_hacker_list",properties)
       }
  }
  def run() : Unit = {
    this.uploadJar()

    this.initConfig()
    this.initParameters()

    this.execute()
    if(this.outputPath.get.toLowerCase().indexOf("hdfs:") > -1 )  this.moveFilesToOfficalPath()

 }

}
