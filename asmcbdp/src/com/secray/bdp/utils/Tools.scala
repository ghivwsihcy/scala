package  com.secray.bdp.utils

object Tools {

  def genSGMethod()
  {
     val field = "id,numId,appId,userId,organization,organizationId,userName,operateTime,terminalId,operateType,operateResult,errorCode,operateName,operateCondition,operateDate,areaCode,areaName,provName,typeProvence,flagWork,flagTime"
     val fields = field.split(",")
     import com.secray.bdp.implicits.ArrayImplicit._

     fields.map {
                  x =>
                     val fletterUpper = x.formatFirstLetterUpper(true).get
                     val m = "def get"+fletterUpper+" : String = { \n"+
                     "   this."+x+"\n"+
                     " }\n"+
                     " \n"+
                     "  def set"+fletterUpper+"("+x+" : String) : Unit = {\n"+
                     "    this."+x+" = "+x+"\n"+
                     " }\n"
                     (m)

                }.foreach (println)
  }

  def main(args: Array[String]): Unit = {
       Tools.genSGMethod()
   }
}
