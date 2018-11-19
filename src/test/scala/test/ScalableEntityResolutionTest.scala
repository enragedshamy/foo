package test

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import textanalyse._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScalableEntityResolutionTest extends FunSuite with BeforeAndAfterAll{
 
  var conf:org.apache.spark.SparkConf=_
  var sc:SparkContext=_
  
  var entityResolutionScalable:ScalableEntityResolution= _
  
  val dat1= "Amazon.csv"
  val dat2= "Google.csv"
  val stopwordsFile= "stopwords.txt"
  val goldStandardFile="Amazon_Google_perfectMapping.csv"
  
  
  override protected def beforeAll() {
    
    conf= new SparkConf().setMaster("local[4]").setAppName("BelegEntityResolutionScalable")
    conf.set("spark.executor.memory","6g")
    conf.set("spark.storage.memoryFraction","0.9")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.driver.allowMultipleContexts", "true")
      
    sc= new SparkContext(conf)
    
    entityResolutionScalable= new ScalableEntityResolution(sc, dat1, dat2, stopwordsFile, goldStandardFile)  
  }
 
  test("Intialisation"){
 
    assert(entityResolutionScalable.amazonTokens.count===1363)
    assert(entityResolutionScalable.googleTokens.count===3226)
    assert(entityResolutionScalable.idfDict.size===17078)
    assert(entityResolutionScalable.goldStandard.count===1300)
  }
  
  test("Test Scalable TFIDF Calculation"){
    
    assert(entityResolutionScalable.amazonWeightsRDD.count===1363)
    assert(entityResolutionScalable.googleWeightsRDD.count===3226)
  }
  
  test("Invert Function Test"){
    
    val in= ("hello",Map(("dies"->1.0), ("ist"->2.0), ("ein"->3.0),("Test"->4.0)))
    val out= List(("dies","hello"), ("ist","hello"), ("ein","hello"), ("Test","hello"))
    assert(ScalableEntityResolution.invert(in)===out)
  }
  
  test("Test Inverse Index Creation"){
    
    entityResolutionScalable.buildInverseIndex
    assert(entityResolutionScalable.amazonInvPairsRDD.count===111387)
    assert(entityResolutionScalable.googleInvPairsRDD.count===77678)  
  }
  
  test("Swap Test"){
    
    val res=ScalableEntityResolution.swap(("say",("hello","world")))
    assert(res===(("hello","world"),"say"))
  }
  
  test("Test determine common tokens"){
    
    entityResolutionScalable.determineCommonTokens
    assert(entityResolutionScalable.commonTokens.count===2441100)
    // alle waeren 4397038 
  }
  
  test("Test Similarity Calculation on full dataset"){
     
    entityResolutionScalable.calculateSimilaritiesFullDataset
    val similarityTest = entityResolutionScalable.similaritiesFullRDD.filter(x=> ((x._1._1.equals( "b00005lzly")) && 
          (x._1._2.equals( "http://www.google.com/base/feeds/snippets/13823221823254120257")))).collect()
    assert(similarityTest.size=== 1)
    assert(Math.abs(similarityTest(0)._2 - 4.286548414e-06) < 0.000000000001)
    assert(entityResolutionScalable.similaritiesFullRDD.count=== 2441100, "incorrect similaritiesFullRDD.count()")
  }
  
  test("Analyse Dataset"){
    
    entityResolutionScalable.analyseDataset
    assert(entityResolutionScalable.trueDupSimsRDD.count===1300)
  
    val similarityTest = entityResolutionScalable.similaritiesFullRDD.filter(x=> ((x._1._1.equals( "b00005lzly")) &&  
          (x._1._2.equals( "http://www.google.com/base/feeds/snippets/13823221823254120257")))).collect()
      assert(similarityTest.size=== 1)
      
    /*
     * *************************************************************
     */
  }
  
  test("Test Analysis"){
    
    val res1= entityResolutionScalable.falsepos(0.9,entityResolutionScalable.fpCounts)
    assert(res1===22)
    val res2= entityResolutionScalable.falseneg(0.9)
    assert(res2===1263)
  }
  
  test("Accumulator Test"){
    val l=Range(1,5).toVector
    val accvec = sc.accumulator(VectorAccumulatorParam.zero(l))(VectorAccumulatorParam)
    assert(accvec.value===Vector(0,0,0,0))
    val ll= Vector(1,2,3,4)
    accvec+=ll
    assert(accvec.value===Vector(1,2,3,4))
    accvec+=ll
    assert(accvec.value===Vector(2,4,6,8))
  }
  
  override protected def afterAll() {

     if (sc!=null) {sc.stop; println("Spark stopped......")}
     else println("Cannot stop spark - reference lost!!!!")
  } 
}