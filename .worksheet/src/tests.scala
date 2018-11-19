object tests {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(58); 
  println("Welcome to the Scala worksheet")}
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.Row
	import textanalyse._
	import java.io.FileOutputStream
	import org.apache.spark.AccumulatorParam

/*
class SearchFunctions( val query:String) extends Serializable{
 
  def isMatch(s:String):Boolean ={

    s.contains(query)
  }
	
 	def getMatchesFunctionReference(rdd:RDD[String]):RDD[Boolean]={
	
 	  // problem: isMatch means this.isMatch), so we pass all of this
    rdd.map(isMatch)
  }

 	def getMatchesFieldReference(rdd:RDD[String]):RDD[Array[String]]={
	
 	  // problem: isMatch means this.isMatch), so we pass all of this
    rdd.map(x=>x.split(query))
  }

 	def getMatchesNoReference(rdd:RDD[String]):RDD[Array[String]]={

    // Safe: Extracts just the field we need into a local variable
    val query_ = this.query
    rdd.map(_.split(query_))
 	}
}


  val zeros:Vector[Int]= Vector.fill(5){0}
  val nthresholds =100
    val thresholds= for (i<- 1 to nthresholds) yield i/nthresholds.toDouble
    
  def set_bit(x:Int, value:Int, length:Int):Vector[Int]={

      Vector.tabulate(length){i => {if (i==x) value else 0}}
   }
   
   set_bit(3,2,10)
    

  val v=Vector.fill(10){0}

	v(3)


  object ListAccumulatorParam extends AccumulatorParam[List[String]]{
    
    def zero(initialValue: List[String]): List[String] = {
      List():List[String]  //Vector.zeros(initialValue.size)
  	}
  	def addInPlace(v1: List[String], v2: List[String]): List[String] = {
    	v1 ++ v2
  	}
 }
 
  val conf= new SparkConf().setMaster("local[4]").setAppName("Beleg3EntityResolution")
 
  val sc= new SparkContext(conf)
 
  val resultList = sc.accumulator(List():List[String])(ListAccumulatorParam)
 
  val l= List("hallo","die","ist","ein","Test")
  val rdd= sc.parallelize(l)
  rdd.foreach(x=>resultList+=List(x))
  
}*/
/*
val l=""""b000jz4hqo","http://www.google.com/base/feeds/snippets/18441480711193821750""""

val ls="""^(.+),(.+)"""
val lt="hello,world"
lt.split(ls)

val s_regex="""^"(.+)","(.+)""""
l.split(s_regex)
 

	val split_regex="""\W+"""
	 val str="!!!!123A/456_B/789C.123A"
	str.split(split_regex)



  val DATAFILE_PATTERN = """^(.+),"(.+)",(.*),(.*),(.*)""".r
  val t=""""b000jz4hqo","clickart 950 000 - premier image pack (dvd-rom)",,"broderbund",0"""
  val t2=""""id","title","description","manufacturer","price""""
	
	def parseLine(line:String):(Row,Int)={
	
  	line match {
  		case DATAFILE_PATTERN(id,title,
  					description,manufacturer,price) => if (id=="\"id\"")
  																								(Row(line),0)
  																							else
  																								(Row(id,title,description,manufacturer,price),1)
  		case _ => (Row(t),0)
 		}
  }
  
  
  // "id","name","description","manufacturer","price"
  
  val s=parseLine(t2)
  s._1(0)
  
  s._1.getString(0)
                                                  
  for (i <- s._1.getString(0) if i!='\"') yield i

	def tokenizeString(s:String):String=
    ""
*/}
