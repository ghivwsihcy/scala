package  com.secray.bdp.utils

import java.util.regex.Matcher;
import java.util.regex.Pattern;

object StringUtil {

   def  split( src : String) : Option[Array[String]] = {
     split(src,",")
   }


  def  split( src : String, exp  : String) : Option[Array[String]] = {
    	if(src == null || src.trim().length()==0 ||
    	   exp == null || exp.trim().length()==0)  None
    	else

    	  Option.apply(src.split(exp,-1))
    }

	def  convertUnicode(str: String,srcCharSet: String,destCharSet: String) : Option[String] = {
	  try
		{

			val  midStrs = str.getBytes(srcCharSet)
			Some(new String(midStrs,destCharSet))
		}
		catch{
		  case e: Exception =>  e.printStackTrace();None
		}

	}
	def  isChineseChar( str : String) : Boolean = {
        var temp = false
        val p=Pattern.compile("[\u4e00-\u9fa5]")
        val m=p.matcher(str)
        if(m.find()){
            temp =  true
        }
        temp
    }
	 def  length( value : String) :Int = {
	        var valueLength = 0
	        val chinese = "[\u0391-\uFFE5]"
	        /* 获取字段值的长度，如果含中文字符，则每个中文字符长度为2，否则为1 */
	        for ( i <- 0 until  value.length()) {
	            /* 获取一个字符 */
	            val temp = value.substring(i, i + 1)
	            /* 判断是否为中文字符 */
	            if (temp.matches(chinese)) {
	                /* 中文字符长度为2 */
	                valueLength += 2
	            } else {
	                /* 其他字符长度为1 */
	                valueLength += 1
	            }
	        }
	        valueLength
	    }

  def getFields(sql : Option[String]) : Option[String] = {

      if( sql.isEmpty)  None
      val fieldSql = sql.get.substring(0,sql.get.toUpperCase().indexOf("FROM"))
      Some(fieldSql.replaceAll(",['|\\d+]", "*").split(",").map { x =>
              if(x.trim().length() > 0) {
                val field = x.trim().reverse
                val pos = field.indexOf(" ")
                //println(x.trim()+" -> "+pos)
                if(pos == -1) field.reverse
                else {
                  println(pos+" -> "+ field.substring(0,pos).reverse)
                  field.substring(0,pos).reverse
                }
              }
      }.mkString(","))
  }
  def main(args: Array[String]): Unit = {

   }
}
