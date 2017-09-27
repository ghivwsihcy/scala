package  com.secray.bdp.utils

import java.util.Calendar
import java.text.SimpleDateFormat

object DateUtil {

  val DATE = "yyyy-MM-dd HH:mm:ss"
	val SHORTDATE = "yyyyMMddHHmmss"
	val SHORTDATEMIL = "yyyyMMddHHmmssSSS"
	val LONGDAY = "yyyy-MM-dd"
	val SHORTDAY = "yyyyMMdd"
	val LONGMONTH = "yyyy-MM"
	val SHORTMONTH = "yyyyMM"

	val YEAR = "yyyy"
	val MONTH = "MM"
	val DAY = "dd"
	val HOUR = "HH"
	val MINUTE = "mm"
	val SECOND = "ss"

	val SMONTH = "M"
	val SDAY = "d"
	val SHOUR = "H"
	val SMINUTE = "m"


	 def formatDateStr(formatExp : String, dayMonthNum : Int) : String =
	 {

		     val c = Calendar.getInstance()

		     if(  DateUtil.DAY.equals(formatExp) || DateUtil.DATE.equals(formatExp)
		    	|| DateUtil.LONGDAY.equals(formatExp) || DateUtil.SHORTDAY.equals(formatExp))
		     {
		    	 c.add(Calendar.DAY_OF_MONTH, dayMonthNum);
		     }
		     else if( DateUtil.MONTH.equals(formatExp) || DateUtil.LONGMONTH.equals(formatExp)
		    		 || DateUtil.SHORTMONTH.equals(formatExp) || DateUtil.SMONTH.equals(formatExp))
		     {
		    	 c.add(Calendar.MONTH, dayMonthNum)
		     }
		     else if( DateUtil.YEAR.equals(formatExp))
		     {
		    	 c.add(Calendar.YEAR, dayMonthNum)
		     }
		     else if( DateUtil.DAY.equals(formatExp) || DateUtil.SDAY.equals(formatExp))
		     {
		    	 c.add(Calendar.DAY_OF_MONTH, dayMonthNum)
		     }
		     val  df = new SimpleDateFormat(formatExp)
		     df.format(c.getTime())

	 }

  /**
	  *
	 * 函数名    : formatCurDateStr
	 * 功   能     : 把当前日期转换成格式的字符 串
	 * 参   数    : @formatExp ：yyyy-MM，yyyy-MM-dd,yyyy-MM-dd HH:mm:ss
	 * 参   数    : @return
	 * 参   数    : @throws ParseException    设定文件
	 * 返回值	 : String    返回类型
	 * 编写者    : root
	 * 编写时间 : 2016年9月21日 下午4:01:26
	  */
	 def  formatCurDateStr( formatExp : String) : String =
	 {
		     var result = null
		     val c = Calendar.getInstance()
		     val df = new SimpleDateFormat(formatExp)
		     return df.format(c.getTime())

	 }
}
