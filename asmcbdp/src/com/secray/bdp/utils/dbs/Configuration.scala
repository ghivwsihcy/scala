package  com.secray.bdp.utils.dbs

import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.File
import java.io.FileOutputStream
import java.util.Properties
import java.io.IOException

class Configuration {
    private var propertie : Properties  = null
    private var inputFile : FileInputStream =_
    private var outputFile : FileOutputStream =_

    //private var


    def this(filePath : String )
    {
    	this()
    	//String path=this.getClass().getResource("").getPath();
        this.propertie = new Properties()
        try {
            //inputFile = new FileInputStream(path+"/"+filePath);

        	 //System.out.println(filePath+" : "+ System. getProperty ("user.dir")
        	//		 +" : "+this.getClass().getResource("").getPath());
             //propertie.load(this.getClass().getResourceAsStream(filePath));
        	 propertie.load(new FileInputStream(new File(filePath)));
            //inputFile.close();
        }
        catch  {
          case fe :  FileNotFoundException => fe.printStackTrace()
          case ioe : IOException =>    ioe.printStackTrace()
        }

    }//end ReadConfigInfo(...)

    /**
     *
     * @param key
     * @return
     */
    def  getProperty(key : String ) : String =
    {
        if(propertie.containsKey(key)){
             propertie.getProperty(key)

        }
        else
             ""
    }//end getValue(...)

    /**
     *
     * @param fileName
     * @param key
     * @return
     */
    def  getValue(fileName : String ,key:  String ) : String =
    {
        try {
            var value = "";
            inputFile = new FileInputStream(fileName);
            propertie.load(inputFile);
            inputFile.close();
            if(propertie.containsKey(key)){
                 propertie.getProperty(key)
            }else
                value
        }
        catch  {
          case fe :  FileNotFoundException => fe.printStackTrace();""
          case ioe : Exception =>    ioe.printStackTrace();""
        }
    }

   /**
    *
    */
    def  clear()  =
    {
        propertie.clear();
    }


    def  setValue(key : String , value : String ) =
    {
        propertie.setProperty(key, value);
    }

 /**
  *
  * @param fileName
  * @param description
  */
    def  saveFile(fileName : String , description : String ) =
    {
        try {
            outputFile = new FileOutputStream(fileName);
            propertie.store(outputFile, description);
            outputFile.close();
        }
        catch  {
          case fe :  FileNotFoundException => fe.printStackTrace()
          case ioe : Exception =>    ioe.printStackTrace()
        }
    }
}
