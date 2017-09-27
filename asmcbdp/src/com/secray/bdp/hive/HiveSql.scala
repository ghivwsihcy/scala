package  com.secray.bdp.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SQLContext
import java.util.Properties

object HiveSql {

   def queryDb()
  {
    val warehouseLocation = "hdfs://192.168.12.9:8020/apps/hive/warehouse"
    val sparkConf = new SparkConf().setMaster("spark://192.168.12.9:17077").setAppName("spark sql test");
    sparkConf.set("spark.sql.warehouse.dir", warehouseLocation)
    val sc = new SparkContext(sparkConf);

    val sqlContext = new SQLContext(sc);

    val url = "jdbc:mysql://192.168.12.12:3306/xdtrdata?user=root&password=123456";
   val prop = new Properties();
   //val df = sqlContext.read.jdbcsqlContext.read.jdbc(url, "tb_conf_column", prop);
    var df  = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://192.168.12.12:3306/xdtrdata",
                                                         "driver" -> "com.mysql.jdbc.Driver",
                                                         "dbtable" -> "tb_conf_column",
                                                         "user" -> "root",
                                                         "password" -> "123456")).load()


    //var df = sqlContext.read.format("jdbc").options(url= "jdbc:mysql://192.168.12.12:3306/xdtrdata?user=root&password=123456", dbtable="tb_conf_column",driver="com.mysql.jdbc.Driver").load()
    println("第一种方法输出："+df.count());
   // println("1.------------->" + df.count());
    println("1.------------->" + df.rdd.partitions.size);
    //df.collect().foreach(println)

    sc.stop()
  }
   def queryHive()
   {
      val warehouseLocation = "hdfs://192.168.12.9:8020/apps/hive/warehouse"
      val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("spark://192.168.12.9:17077")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate();
     import spark.implicits._
     import spark.sql

     System.out.println("---------lb0--------------");
     sql("show databases").collect().foreach(println)
     //sql("use ok")
     System.out.println("---------lb1--------------");
      sql("show tables").collect().foreach(println)
    // sql("SELECT COUNT(*) FROM tb_asmc_md_app_oper_log").show()
     System.out.println("---------lb3--------------");
     spark.stop()
   }

   def main(args:Array[String]) {

    /*//设置用户名
    System.setProperty("user.name", "root");
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    //此处不需要设置master，方便到集群上，能测试yarn-client , yarn-cluster，spark 各种模式
    val sc=new SparkConf().setAppName("spark sql hive").setMaster("spark://192.168.12.9:17077");
    System.out.println("---------lb1--------------");
    val sct=new SparkContext(sc);
    System.out.println("---------lb2--------------");
    //得到hive上下文
    val hive = new org.apache.spark.sql.hive.HiveContext(sct);
    //执行sql，并打印输入信息
    hive.sql("show tables ").collect().foreach(println);
    //关闭资源
    sct.stop();*/

   //queryDb();
    queryHive()
  }

}



