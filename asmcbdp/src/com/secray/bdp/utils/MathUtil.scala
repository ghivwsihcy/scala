package  com.secray.bdp.utils

import scala.util.Random

/**
  * Created by root on 2017/6/28 0028.
  */
object MathUtil {

  def random(n:Int)={
    var resultList:List[Int]=Nil
    while(resultList.length<n){
      val randomNum=(new Random).nextInt(10)
      if(!resultList.exists(s=>s==randomNum)){
        resultList=resultList:::List(randomNum)
      }
    }
    resultList.mkString("")
  }

  def main(args: Array[String]): Unit = {
      println(MathUtil.random(3)+"->"+(new Random).nextInt(1000))
  }
}
