import com.secray.bdp.utils.DateUtil

/**
  * Created by root on 2017/9/9 0009.
  */
object DateUtilUnit {
  def main(args: Array[String]): Unit = {
    println(DateUtil.formatDateStr(DateUtil.SHORTDAY, -1))
  }
}
