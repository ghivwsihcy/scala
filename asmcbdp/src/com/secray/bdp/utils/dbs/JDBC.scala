package  com.secray.bdp.utils.dbs

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement

import com.secray.bdp.log.Logging

import scala.util.control.Breaks
import com.secray.bdp.stat.mapduce.base.Constant
import java.sql.SQLException


class JDBC(jdbcType : Int) extends Logging{


	private var connection : Connection  = null
	//private Statement statements = null;
	protected var statement: Statement  = null
	private var rs : ResultSet  = null
	//private var jdbcType=0
	private var DBDRIVER :String = null
	private var dbType :Int = 0
	protected var schema :String = null
	private  val dbNames =Array("ORACLE","DB2","MYSQL","HIVE")

	def  getConnection() : Connection =  {
		this.connection
	}

	def  setConnection(connection : Connection ) =  {
		 this.connection = connection
	}


    def   getSchema() : String =
    {
    	  this.schema

    }

    def   getDbType() : String = {
		  // this.dbNames[this.dbType];
      ""
	   }

	def  connect() : Statement =
	{
		val  rc = new Configuration("./conf/jdbc.properties")
		this.DBDRIVER = rc.getProperty("jdbc.hive.Driver")
		var URL = rc.getProperty("jdbc.hive.URL")
		var userName = rc.getProperty("jdbc.hive.userName")
		var password = rc.getProperty("jdbc.hive.password")

		if(this.jdbcType == Constant.DB)
		{
			DBDRIVER = rc.getProperty("jdbc.db.Driver");
			URL = rc.getProperty("jdbc.db.URL");
			userName = rc.getProperty("jdbc.db.userName");
			password = rc.getProperty("jdbc.db.password");
		}
		else if(this.jdbcType == Constant.HF)
		{
			DBDRIVER = rc.getProperty("jdbc.hf.Driver");
			URL = rc.getProperty("jdbc.hf.URL");
			userName = rc.getProperty("jdbc.hf.userName");
			password = rc.getProperty("jdbc.hf.password");
		}
		else if(this.jdbcType == Constant.MS)
		{
			DBDRIVER = rc.getProperty("jdbc.mysql.Driver");
			URL = rc.getProperty("jdbc.mysql.URL");
			userName = rc.getProperty("jdbc.mysql.userName");
			password = rc.getProperty("jdbc.mysql.password");
		}

		val forBreak = new Breaks()
		//取得数据库类型
		for( i <- 0 until  dbNames.length )
		{
		     if(this.DBDRIVER.toUpperCase().indexOf("."+dbNames(i)+".") > 0)
	    	 {
		    	 dbType = i;
		    	 forBreak.break()
	    	 }
		}
		log.info("connect : "+URL+"\n "+userName+"\n "+password);
		//如果数据库是MYSQL,取得连接串中的数据库名称
		if("MYSQL".equals(this.dbNames(this.dbType))){
			schema =  URL.substring(URL.lastIndexOf("/")+1);
    }

		Class.forName(DBDRIVER).newInstance();

		//Connect to the database
		connection = DriverManager.getConnection(URL, userName, password);
		this.setConnection(connection);
		statement = connection.createStatement();
		return statement;

	}

	def  query(sql : String ) : ResultSet =
	{

			 statement.executeQuery(sql)

  }
	def  execute(sql : String ) : Boolean =
	{
		try {

			//Obtain a statement object
			//statement = connection.createStatement();
			statement.execute(sql)

			}
		catch {
		  case e: Exception => e.printStackTrace();		false
		}
  }

	def  truncate(tableName : String ):Int =
     {
    	 if(tableName == null || tableName.trim().length() ==0 )  -1
    	 this.update("truncate table "+tableName)
     }
	  /**
   *
  * 函数名    : dropTable
  * 功   能    : 删除表
  * 参   数    : @param tableName
  * 参   数    : @return
  * 参   数    : @throws SQLException    设定文件
  * 返回值	 : int    返回类型
  * 编写者    : root
  * 编写时间 : 2016年9月7日 上午9:15:32
   */
 def  dropTable( tableName : String ) :Int  = {

     this.statement = connection.createStatement();
 	   this.statement.executeUpdate("drop table " + tableName);
 }

	def  commit() =
	{
	  try
	  {
		  connection.commit()
	  }
	  catch{
	    case sqle: SQLException => sqle.printStackTrace()
	  }
	}

	def  rollback() =
	{
	  try
	  {
		  connection.rollback()
	  }
	  catch{
	    case sqle: SQLException => sqle.printStackTrace()
	  }
	}


	def  update(sql : String ) : Int =
	{
			statement.executeUpdate(sql)
	}

 def  close() =
 {
	//Time to close everthing up.

   try
   {
  	   if(rs !=null)  rs.close();
  	   if( statement != null ) {statement.close();	}
  	   //if( statements != null ) {statements.close();	}

  		if( connection != null ) {
  				connection.close();
  		}
   }
   catch{
     case sqle: SQLException => sqle.printStackTrace()
   }
  }
}
