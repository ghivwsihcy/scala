import com.secray.bdp.utils.DateUtil

/**
  * Created by root on 2017/9/11 0011.
  */
object StringUnit {
  def main(args: Array[String]): Unit = {
    val statDayMon="20170911"
    val year = statDayMon.substring(0,4)
    val month = statDayMon.substring(4,6)
    val day = statDayMon.substring(6,8)
    println(year+" :"+month+" :"+day)
  }
}
