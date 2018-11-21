package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner
import textanalyse._

@RunWith(classOf[JUnitRunner])
class getTokensEntityResolutionTest extends FunSuite with BeforeAndAfterAll {

  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _
  var entityResolution: EntityResolution = _

  val dat1 = "Amazon_small.csv"
  val dat2 = "Google_small.csv"
  val stopwordsFile = "stopwords.txt"
  val goldStandardFile = "Amazon_Google_perfectMapping.csv"

  var googleRecToToken: RDD[(String, List[String])] = _
  var amazonRecToToken: RDD[(String, List[String])] = _

  override protected def beforeAll() {

    conf = new SparkConf().setMaster("local[4]").setAppName("BelegEntityResolution")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.storage.memoryFraction", "0.8")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.driver.allowMultipleContexts", "true")

    sc = new SparkContext(conf)
    entityResolution = new EntityResolution(sc, dat1, dat2, stopwordsFile, goldStandardFile)

  }

  test("Initialization Test") {
    println("Google Small:")
    entityResolution.googleRDD.take(5).foreach(println)

    println("Amazon Small:")
    entityResolution.amazonRDD.take(5).foreach(println)

    assert(entityResolution.amazonRDD.count === 200)
    assert(entityResolution.googleRDD.count === 200)

    assert(entityResolution.stopWords.size === 127)
    entityResolution.goldStandard.take(10).foreach(println)
    assert(entityResolution.goldStandard.count() === 1300)
  }

  test("Test tokenizeString 0") {

    val s = ""
    val words_s = List()

    val r = Utils.tokenizeString(s)
    assert(r.length === 0)
    assert(r === words_s)
  }

  test("Test tokenizeString 1") {

    val s = "This 88 is! a,Test! The result !!!should be: 8 Words"
    val words_s = List("8", "88", "a", "be", "is", "result", "should", "test", "the", "this", "words")

    val r = Utils.tokenizeString(s)
    assert(r.length === 11)
    assert(r.sorted === words_s)
  }

  test("Test tokenizeString 2") {

    val s = "This is another test. It contains a lot of words which are also in string 1."
    val words_s = List("1", "a", "also", "another", "are", "contains", "in", "is", "it", "lot", "of", "string", "test", "this", "which", "words")
    val r = Utils.tokenizeString(s)
    assert(r.length === 16)
    assert(r.sorted === words_s)
  }

  test("Test tokenizeString 3") {

    val s = "!!123A!/456_B_12/987C.123d"
    val words_s = List("123a", "123d", "456_b_12", "987c")
    val r = Utils.tokenizeString(s)
    assert(r.length === 4)
    assert(r.sorted === words_s)
  }

  test("Test tokenize 1") {

    val s = "Being at the top of the pops!"
    val words_s = List("pops", "top")
    val r = EntityResolution.tokenize(s, entityResolution.stopWords)
    assert(r.size === 2)
    assert(r.sorted === words_s)
  }

  test("Test tokenizer") {

    amazonRecToToken = entityResolution.getTokens(entityResolution.amazonRDD)
    googleRecToToken = entityResolution.getTokens(entityResolution.googleRDD)
    val nr_amazon = entityResolution.countTokens(amazonRecToToken)
    val nr_google = entityResolution.countTokens(googleRecToToken)
    assert((nr_amazon + nr_google) === 22520)
  }

  test("Test Biggest Record") {

    val rec = entityResolution.findBiggestRecord(amazonRecToToken)
    assert(rec._1 === "b000o24l3q")
    assert(rec._2.size === 1547)
  }

  test("Test Calculation of Term Frequency") {

    val s = "This is test Test and this is another test test."
    val expected = Map("test" -> 0.4, "this" -> 0.2, "is" -> 0.2, "another" -> 0.1, "and" -> 0.1)
    val res = EntityResolution.getTermFrequencies(Utils.tokenizeString(s))
    assert(res === expected)
  }

  test("Test Create Corpus") {

    entityResolution.createCorpus
    val nr = entityResolution.corpusRDD.count
    assert(nr === 400)
  }

  test("Test Calculate Inverse Document Frequencies") {

    val idfs_SmallRDD = entityResolution.calculateIDF
    //val res= entityResolution.idfDict.collectAsMap.toMap
    assert(entityResolution.idfDict.size === 4772)
    //entityResolution.idfDict.takeOrdered(10)(Ordering.by[(String,Double),Double](_._2)).foreach(println)

  }

  test("Test TF IDF-Caluclation") {

    val recb000hkgj8k = amazonRecToToken.filter(_._1 == "b000hkgj8k").collect()(0)._2
    val rec_b000hkgj8k_weights = EntityResolution.calculateTF_IDF(recb000hkgj8k, entityResolution.idfDict)
    val expected = Map(("autocad" -> 33.33333333333333), ("autodesk" -> 8.333333333333332), ("courseware" -> 66.66666666666666), ("psg" -> 33.33333333333333),
      ("2007" -> 3.5087719298245617), ("customizing" -> 16.666666666666664), ("interface" -> 3.0303030303030303))
    assert(rec_b000hkgj8k_weights === expected)
  }

  test("Test Dot Product") {

    val v1: Map[String, Double] = Map(("a" -> 4.0), ("c" -> 5), ("b" -> 7))
    val v2: Map[String, Double] = Map(("a" -> 2), ("b" -> 50), ("d" -> 100))
    val res = EntityResolution.calculateDotProduct(v1, v2)
    assert(res === 358)
  }

  test("Test Norm Calc") {

    val vec: Map[String, Double] = Map(("a" -> 4), ("c" -> 5), ("b" -> 7))
    val res = EntityResolution.calculateNorm(vec)
    assert(Math.abs(res - 9.486832980505) < 0.000001)
  }

  test("Test Cosinus Similarity") {

    val v1: Map[String, Double] = Map(("a" -> 4), ("c" -> 5), ("b" -> 7))
    val v2: Map[String, Double] = Map(("a" -> 5), ("c" -> 2), ("e" -> 7))
    val res = EntityResolution.calculateCosinusSimilarity(v1, v2)
    assert(Math.abs(res - 0.35805743701971) < 0.000001)
  }

  test("Test Document Similarity") {

    val res = EntityResolution.calculateDocumentSimilarity("Adobe Photoshop", "Adobe Illustrator", entityResolution.idfDict, entityResolution.stopWords)
    assert(Math.abs(res - 0.0577243382163) < 0.000001)
  }

  test("Test Simple Similarity Calculation") {

    val sim = entityResolution.simpleSimimilarityCalculation
    val res = entityResolution.findSimilarity("b000o24l3q", "http://www.google.com/base/feeds/snippets/17242822440574356561", sim)
    println(res)
    assert(Math.abs(res - 0.000303171940451) < 0.000001)
  }

  test("Test Simple Similarity Calculation with Broadcast") {

    val sim = entityResolution.simpleSimimilarityCalculationWithBroadcast
    val res = entityResolution.findSimilarity("b000o24l3q", "http://www.google.com/base/feeds/snippets/17242822440574356561", sim)
    println(res)
    assert(Math.abs(res - 0.000303171940451) < 0.000001)
  }

  test("Evaluate Model") {

    val (dupCount, avgSDups, avgSNonDups) = entityResolution.evaluateModel(entityResolution.goldStandard)

    assert(dupCount === 146)
    assert(Math.abs(avgSDups - 0.264332573435) < 0.0000001)
    assert(Math.abs(avgSNonDups - 0.00123476304656) < 0.0000001)
  }

  override protected def afterAll() {

    if (sc != null) {
      sc.stop;
      println("Spark stopped......")
    }
    else println("Cannot stop spark - reference lost!!!!")
  }

}

