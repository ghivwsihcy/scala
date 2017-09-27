package  com.secray.bdp.utils

import scala.util.Try
import scala.util.Success

object CommonUtil {
  val NEED_ESC_CHARS = "$,(,),*,+,.,[,],?,\\,^,{,},|"

  def parseInt(src : String,default : Int) : Int = {
     if(src == null || src.trim().length ==0 )  default
     if("\\N".equals(src)) default
     val dest = Try(src.toInt)
     dest match{
        case Success(_) => src.toInt
        case _ => default
     }
  }

  def parseInt(src : String) : Int = {
      parseInt(src,0)
  }

   def convertToNulls(src: Any) : Any = {
      val defaultValue = src match{
         case s:String => ""
         case i:Integer => 0
         case _=> null
      }
       convertToNulls(src,defaultValue)
    }
   def convertToNulls(src: Any,default : Any) : Any = {
       val result = src match{
         case s:String =>
             val str = src.toString()
             if(str != null || str.trim().length()==0 || "\\N".equals(str))  default.toString() else str
         case i:Integer =>
             if(src != null ) src.toString().toInt else  default.toString().toInt
         case _ =>     if(src == null) default else src
       }

       result
    }
    def convertToNull(src: String,default : String) : String = {
       if(src == null || src.trim().length()==0 || "\\N".equals(src))  default
       else   src
    }

     def convertToNull(src: String) : String = {
       convertToNull(src,"")
    }
    /**
	  *
	 * 函数名    : addEscapeCHar
	 * 功   能    : 添加转义符号
	 * 参   数    : @param src
	 * 参   数    : @return    设定文件
	 * 返回值	 : String    返回类型
	 * 编写者    : root
	 * 编写时间 : 2016年10月10日 下午2:43:19
	  */
	 def  addEscapeCHar( src : String) : String = {

		 var result = "";
		 var isMatch = false;
		 var charStr = "";
		 val escpChars = NEED_ESC_CHARS.split(",");

		 for(  j <- 0  to  src.length -1)
		 {
			 isMatch = false;
			 charStr = src.charAt(j).toString()
			 for(  i <- 0  to  escpChars.length -1 if(!isMatch))
			 {

				    if(charStr.equals(escpChars(i))){
				    	result += charStr.replace(escpChars(i), "\\" + escpChars(i))
				    	isMatch = true

				    }
			 }
			 if(!isMatch) result += charStr;
		 }
		 result
	 }

	 def isInt(src :String) : Boolean = {
	    val r1=scala.util.Try(src.toInt)
      val result = r1 match {
        case Success(_) => 1 == 1
        case _ =>  0 == 1
	    }
	    result
	 }

}
