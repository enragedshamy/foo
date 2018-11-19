package textanalyse

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object Utils extends Serializable{
  
  val FILENAME_ACCESS_PATTERN ="""^(.+)\/(\w+\.\w+)$""".r
  val DATAFILE_PATTERN = """^(.+),"(.+)",(.*),(.*),(.*)""".r
  val GOLDFILE_PATTERN = """^(.+),"(.+)""".r
  val split_regex="\\W+"
  
  def getData(fileName:String, sc:SparkContext):RDD[(String,String)]={
          
    val url=getClass.getResource("/"+fileName).getPath
    val path = url match{
      case FILENAME_ACCESS_PATTERN(path, name) => (path, name)
      case _ => ("","")
    }
    val rawdata= sc.textFile("file://"+url).map(parseLine).cache
    val failed= rawdata.filter(_._2== -1).cache
    if (failed.count>0) {println("Failed to parse lines:");failed.take(10).foreach(println)}
    rawdata.filter(_._2==1).map(_._1.asInstanceOf[(String,String)]).cache
  }
  
  def getStopWords(fileName:String):Set[String]={
    
    val url= getClass.getResource("/"+fileName).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    val result= iter.foldLeft(Set():Set[String])((set,el)=> set+el)
    src.close()
    result
  }
  
  def getGoldStandard(fileName:String, sc:SparkContext):RDD[(String, String)]={
    
    val url=getClass.getResource("/"+fileName).getPath
    val path = url match{
      case FILENAME_ACCESS_PATTERN(path, name) => (path, name)
      case _ => ("","")
    }

    val rawdata= sc.textFile("file://"+url).map(parseGoldStandardLine).cache
    val failed= rawdata.filter(_._2== -1).cache
    if (failed.count>0) {println("Failed GoldStandard to parse lines:");failed.take(10).foreach(println)}
    rawdata.filter(_._2==1).map(_._1.asInstanceOf[(String,String)]).cache
  }
  
  def parseLine(line:String):(Any,Int)={
	
  	line match {
  		case DATAFILE_PATTERN(id,title,description,manufacturer,price) => 
  		      if (id.equals("\"id\"")) (line,0)
  						else
  						  ((deleteQuotes(id),title+" "+description+" "+manufacturer),1)
  		case _ => (line,-1)
 		}
  }
  
  def parseGoldStandardLine(line:String):(Any,Int)={
    line match {
  		case GOLDFILE_PATTERN(amazonID, googleID) => 
  		      if (amazonID.equals("\"idAmazon\"")) ((line),0)
  						else
  						  ((deleteQuotes(amazonID)+" "+deleteQuotes(googleID),"gold"),1)
  		case _ => ((line),-1)
 		}
  }
  
  def deleteQuotes(s:String):String=
    for (i <- s if i!='\"') yield i
    
  def tokenizeString(s:String):List[String]={
    
    val words= s.toLowerCase.split(split_regex).toList
    words.filter(_!="")
  } 
}