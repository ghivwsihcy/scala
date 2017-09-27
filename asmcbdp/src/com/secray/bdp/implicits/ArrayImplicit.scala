package  com.secray.bdp.implicits

import org.apache.hadoop.fs.FileStatus
import com.secray.bdp.utils.CommonUtil

class ArrayImplicit(array: Array[String]) {
  def getOptValue(index:Int): Option[String] =
    if (array.length > index) Some(array(index)) else None



}
class ArrayFileImplicit(array: Array[FileStatus]) {


  def getOptValue(index:Int): Option[String] = {
    if (array.length > index) Some(array(index).getPath.getName) else None
  }
  def convertToString( excludeFileName : String) : Option[String] = {
      if (array != null && array.length > 0) {
        var eFileName = CommonUtil.convertToNull(excludeFileName).toUpperCase()
        val result = array.filter( !_.getPath.getName.toUpperCase().equals(eFileName) ).map{ x => x.getPath.toUri().toString() }
        Some(result.mkString(","))
      }
      else None
  }

}
  class FirstLetterUpper(field :String) {

   def formatFirstLetterUpper(isFirstUpper : Boolean): Option[String] = {
     if ( field != null && field.trim().length() > 0 ){
       var result = field
       val idx = field.indexOf("_")
       if(idx > 0 )
       {
         val rpStr = field.substring(idx, idx+2)
         result = field.replace(rpStr, rpStr.substring(1,2).toUpperCase())

       }
      if( isFirstUpper ) Some(result.substring(0,1).toUpperCase()+result.substring(1))
      else Some(result)

     }
     else None
   }



 }




object ArrayFileImplicit{
  implicit def getArrayFileImplicit(array: Array[FileStatus]) = new ArrayFileImplicit(array)
}
object ArrayImplicit{

  implicit def getArrayImplicit(array: Array[String]) = new ArrayImplicit(array)
  implicit def  formatFirstLetterUpperImplicit(field :String) = new FirstLetterUpper(field)

  def main(args: Array[String]) {
     //
      val v =  Array("a","b")
      println(v.getOptValue(0).get)
  }



}

