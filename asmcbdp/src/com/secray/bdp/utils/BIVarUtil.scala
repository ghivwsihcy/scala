package  com.secray.bdp.utils

import com.secray.bdp.log.Logging

object BIVarUtil extends Logging {

    def getBuldInVarValue(srcVar : String,statDayMon : String) : String = {
        val buildInVarTrim = srcVar.replaceAll("\\[|\\]|\\s+", "")
        val bInExpress = buildInVarTrim.split("\\+|\\-", -1)
        var dayNum = "0"
		    var operator = ""
		    var formatType = DateUtil.SHORTDAY;

		    if(bInExpress.length > 1) {
		      operator = buildInVarTrim.replace(bInExpress(0), "").substring(0,1)
		      if("+".equals(operator)){
		         dayNum = bInExpress(1)
		      }
		      else
		        dayNum = operator + bInExpress(1)

		    }

        if ("SHORT_CUR_DATE".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.SHORTDAY;
    		} else if ("LONG_CUR_DATE".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.LONGDAY;
    		} else if ("SHORT_CUR_MONTH".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.SHORTMONTH;
    		} else if ("LONG_CUR_MONTH".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.LONGMONTH;
    		}else if ("YEAR".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.YEAR;
    		}
    		else if ("MONTH".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.MONTH;
    		}
    		else if ("DAY".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.DAY;

    		}
    		else if ("SMONTH".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.SMONTH;
    		}
    		else if ("SDAY".equals(bInExpress(0).toUpperCase())) {
    			formatType = DateUtil.SDAY;
    		}
        val statDayMonth = CommonUtil.convertToNull(statDayMon, "")
        if(statDayMonth.length() == 0 )
  		    DateUtil.formatDateStr(formatType, CommonUtil.parseInt(dayNum,0))
  		  else {
  		     bInExpress(0).toUpperCase() match {
  		       case "YEAR" =>  statDayMonth.substring(0, 4)
  		       case "MONTH" =>  statDayMonth.substring(4, 6)
  		       case "SMONTH" =>  statDayMonth.substring(4, 6).toInt+""
  		       case "DAY" =>  statDayMonth.substring(6, 8)
  		       case "SDAY" =>  statDayMonth.substring(6, 8).toInt+""
  		     }
  		  }
    }

   /**
   * 函数名    : formatVar
   * 功   能    : 格式化输入和输出目录中的变量,并把变量替换成statDayMon对应的值
   * 参   数    : @param src :
   * 参   数    : @param statDayMon  : 统计日期或者月
   * 返回值	: String
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
    def formatVar( src : String ,statDayMon : String ) : Option[String] = {
        if(CommonUtil.convertToNull(src, "").length() == 0 ) None
        var result = src;
        //取得内置变量
        val regex = "\\[(.*?)\\]".r
        var sDayMon = statDayMon
        if(src.indexOf("[") > -1 && CommonUtil.convertToNull(statDayMon).length() == 0 ){
           sDayMon = DateUtil.formatCurDateStr(DateUtil.SHORTDAY)
        }
        for(matchString <- regex.findAllIn(src)){
            result = result.replace(matchString.trim(), getBuldInVarValue(matchString.trim(),sDayMon))
        }
        Some(result)
    }

     /**
   * 函数名    : getFormatYMDPath
   * 功   能    : 把[year]/[month]/[day]根据statisMonDay的值格式2017/05/09
   * 参   数    : @param statType : 统计类型
   * 参   数    : @param statDayMon  : 统计日期或者月
   * 参   数    : @param inputPath  : 配置文件中的输入输出路径
   * 返回值	 : String
   * 编写者    : root
   * 编写时间 : 2017年03月09日 下午15:38:26
   */
    def getFormatYMDPath(statType : String,statisMonDay : String,inputPath : String) : String = {
   //  val inputPath = XmlUtil.getThresholdByName(statType+".inputOutPath").get
		  var ioArgs=inputPath.split(",")
		  if(ioArgs(0).indexOf("[") == -1 ) return ""
		  //处理目录中的内置变量
		  ioArgs(0) = BIVarUtil.formatVar(ioArgs(0),statisMonDay).get

		  var inPaths = ioArgs(0).split("/")
		  if(inPaths == null || inPaths.length==0)  ""
		  var result="";
		  var  max  = 3;


		  if(statType.toUpperCase().indexOf("MON") > -1) max=2
		  //处理插入的统计日期或者月份的参数
		  var MonDayArg :  Array[String] = null
		  if(statisMonDay!=null && statisMonDay.trim().length()>0){
			  if(statType.toUpperCase().indexOf("MON")> -1 ){
				  if(statisMonDay.length() == 6)
					  MonDayArg = Array( statisMonDay.substring(0, 4),statisMonDay.substring(4,6))
				  else
					  MonDayArg = inPaths
			  }
			  else
			  {
				  if(statisMonDay.length()==8)
					  MonDayArg = Array(statisMonDay.substring(0, 4),statisMonDay.substring(4,6),statisMonDay.substring(6,8))
				  else
					  MonDayArg = inPaths
			  }
			  inPaths = MonDayArg
		  }
		  //只取路径中日期目录
		  for( i <- -max to -1 ){
			   val dayMons = inPaths(inPaths.length + i).split("=");

			  if(dayMons != null || dayMons.length>0){
				   //result += "/"+(dayMons[dayMons.length-1].length()==1?"0"+dayMons[dayMons.length-1]:dayMons[dayMons.length-1]);
				   if(dayMons(dayMons.length-1).length() == 1)
				      result += "/"+ "0"+dayMons(dayMons.length-1)
				   else
				      result += "/"+ dayMons(dayMons.length-1)
			  }
		  }

		  logInfo("getFormatYMDPath :"+result);
		  result
	}

}
