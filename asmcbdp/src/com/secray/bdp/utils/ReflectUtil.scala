package  com.secray.bdp.utils

import java.lang.reflect.Constructor
import scala.collection.mutable.ArrayBuffer

object ReflectUtil {
  private  var clazz  : Class[_]  = null
  private var instance : Any = null

  def setEntitiyValues( line:String,field :String) : Any  = {
      val values = StringUtil.split(line,"\\$").get
      import com.secray.bdp.implicits.ArrayImplicit.formatFirstLetterUpperImplicit

      val fields = StringUtil.split(field).get.map{ x => x.formatFirstLetterUpper(true).get }

      val clsName = "com.secray.bdp.user.entity.Log"
      ReflectUtil.buildClass(clsName)
      for( i<- 0 until fields.length) {
        println("set"+fields(i)+" -> "+values(i))
         ReflectUtil.invoke("set"+fields(i),classOf[String],values(i))
      }
      instance

  }
  def buildRow() = {
      val clsName = "org.apache.spark.sql.Row"
      val args =List("121",23,"2312",34)
      val row = ReflectUtil.buildClass[org.apache.spark.sql.Row](clsName,args:_*)
      println(row.mkString(","))
  }
  def buildClass(className : String) : Class[_] = {
      clazz = Class.forName(className)

      instance = clazz.newInstance()

      clazz
  }
    def buildClass[T](className : String,ccArgs :  Any *) : T = {
      clazz = Class.forName(className)

      val args = new ArrayBuffer[Class[_]]()
      ccArgs.foreach { x => args += Class.forName(x.getClass().getCanonicalName);println(x.getClass.getCanonicalName) }
      val  cc =  clazz.getDeclaredConstructor(args:_*)


		  cc.newInstance(ccArgs).asInstanceOf[T]


  }

  def invokeNoArgs(method: String) : Object = {
     invoke(method,null,null)
   }
  def invoke(method: String, clsTypes: Class[_],args: Object) : Object = {

     if(clsTypes != null) {
       val m = clazz.getMethod(method, clsTypes)
       m.invoke(instance, args)
     }
     else
     {
       val m = clazz.getMethod(method)
       m.invoke(instance)
     }

  }

  def showGetValues() = {
     clazz.getMethods.filter(_.getName.contains("get")).map{ x => (x.invoke(instance),x.getName)}.foreach { x => println("showGetValues["+x._2+"] : "+x._1) }

  }

  def main(args: Array[String]): Unit = {
       ReflectUtil.buildRow()
   }
}
