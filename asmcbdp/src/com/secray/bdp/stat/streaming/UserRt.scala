package  com.secray.bdp.stat.streaming

import org.apache.spark.streaming.dstream.DStream

import com.secray.bdp.stat.streaming.base.StreamBase
import com.secray.bdp.stat.streaming.base.StreamBaseInf
import com.secray.bdp.stat.mapduce.base.Constant

class UserRt( statType : String,   group : String, topics : String ,numThreads : String)
extends  StreamBase(statType  ,  group , topics  ,numThreads )
with StreamBaseInf
{
    super.setSparkBaseInf(this)
  /**
    *
    * 函数名    : handleData
    * 功   能   : 处理stream接收到的数据流
    * 参   数   : @param  lines
       	 * 参   数   : @param  statType
       	 * 参   数   : @param  escSep
       	 * 参   数   : @param  seperator
    * 返回值    : void
    * 编写者    : root
    * 编写时间  : 2017-06-09 11:19:50 上午 11:19
    */
    def handleData(lines : DStream[String],statType : String,escSep : String,seperator :String)  = {

        lines.print()
    }
}

 object UserRt{
   def main(args: Array[String]): Unit = {
        if( args.length == 2 ) {
            val userRt = new UserRt("UserRT","UserRTGroup",args(0),args(1))
            userRt.run()
        }

    }
  }