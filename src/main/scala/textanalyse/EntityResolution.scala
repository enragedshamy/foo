package textanalyse

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import textanalyse.EntityResolution.computeSimilarity
import textanalyse.Utils.tokenizeString

class EntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String) {

  val amazonRDD: RDD[(String, String)] = Utils.getData(dat1, sc)
  val googleRDD: RDD[(String, String)] = Utils.getData(dat2, sc)
  val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)
  val goldStandard: RDD[(String, String)] = Utils.getGoldStandard(goldStandardFile, sc)

  var amazonTokens: RDD[(String, List[String])] = _
  var googleTokens: RDD[(String, List[String])] = _
  var corpusRDD: RDD[(String, List[String])] = _
  var idfDict: Map[String, Double] = _

  var idfBroadcast: Broadcast[Map[String, Double]] = _
  var similarities: RDD[(String, String, Double)] = _

  def getTokens(data: RDD[(String, String)]): RDD[(String, List[String])] = {
    /*
     * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
     * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
     */
    val stopWords_tmp = stopWords
    data.map(data => (data._1, EntityResolution.tokenize(data._2, stopWords_tmp)))
  }

  def countTokens(data: RDD[(String, List[String])]): Long = {
    /*
     * Zählt alle Tokens innerhalb eines RDDs
     * Duplikate sollen dabei nicht eliminiert werden
     */
    data
      .map(data => data._2.size)
      .fold(0)((a, b) => a + b)
  }

  def findBiggestRecord(data: RDD[(String, List[String])]): (String, List[String]) = {
    /*
     * Findet den Datensatz mit den meisten Tokens
     */
    data
      .sortBy(data => -data._2.size)
      .first
  }

  def createCorpus = {
    /*
     * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
     * (amazonRDD und googleRDD), vereinigt diese und speichert das
     * Ergebnis in corpusRDD
     */
    val amazonRdd_tmp = amazonRDD
    val googleRdd_tmp = googleRDD
    val united: RDD[(String, String)] = amazonRdd_tmp.union(googleRdd_tmp)
    corpusRDD = getTokens(united)
  }


  def calculateIDF = {
    /*
     * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
     * Speichern des Dictionaries in die Variable idfDict
     */
    val corpus_tmp = corpusRDD
    val numberOfDocuments: Double = corpus_tmp.collect().length.toDouble

    var new_way = corpus_tmp
      .map(data => data._2.map(word => word -> data._1))
      .flatMap(data => data)
      .groupByKey()
      .map(data => data._1 -> (numberOfDocuments / data._2.toList.distinct.length))
      .collect()
      .toMap

    idfDict = new_way
  }


  def countExistanceOfWord(word: String): Int = {
    val corpus_tmp = corpusRDD
    corpus_tmp.filter(data => !data._2.contains(word)).collect().length
  }

  def simpleSimimilarityCalculation: RDD[(String, String, Double)] = {

    /*
     * Berechnung der Document-Similarity für alle möglichen
     * Produktkombinationen aus dem amazonRDD und dem googleRDD
     * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
     * steht, an zweiter die GoogleID und an dritter der Wert
     */
    val _amazonRDD = amazonRDD
    val _googleRDD = googleRDD
    val _stopWords = stopWords
    val _idfDict = idfDict
    val cartesianRDD = _amazonRDD.cartesian(_googleRDD)

    cartesianRDD.map(x => {
      computeSimilarity(x, _idfDict, _stopWords)
    })
  }

  def findSimilarity(vendorID1: String, vendorID2: String, sim: RDD[(String, String, Double)]): Double = {
    /*
     * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
     */
    sim
      .filter(x => {
        x._1 == vendorID1 && x._2 == vendorID2
      })
      .first()
      ._3
  }

  def simpleSimimilarityCalculationWithBroadcast: RDD[(String, String, Double)] = {

    val _sc = sc
    val _amazonRDD = amazonRDD
    val _googleRDD = googleRDD
    val _stopWords = stopWords
    val _idfDict = idfDict
    val idfBroadcast = sc.broadcast(_idfDict)
    val cartesianRDD = _amazonRDD.cartesian(_googleRDD)

    cartesianRDD.map(x => {
      computeSimilarity(x, idfBroadcast, _stopWords)
    })
  }

  /*
   *
   * 	Gold Standard Evaluation
   */

  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

    /*
     * Berechnen Sie die folgenden Kennzahlen:
     *
     * Anzahl der Duplikate im Sample
     * Durchschnittliche Consinus Similaritaet der Duplikate
     * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
     *
     *
     * Ergebnis-Tripel:
     * (AnzDuplikate, avgCosinus-SimilaritätDuplikate,avgCosinus-SimilaritätNicht-Duplikate)
     */


    ???
  }
}

object EntityResolution {

  def tokenize(s: String, stopws: Set[String]): List[String] = {
    /*
   	* Tokenize splittet einen String in die einzelnen Wörter auf
   	* und entfernt dabei alle Stopwords.
   	* Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
   	*/
    tokenizeString(s)
      .filter(string => !stopws.contains(string))
  }

  def getTermFrequencies(tokens: List[String]): Map[String, Double] = {
    /*
     * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
     * Menge aller Wörter innerhalb eines Dokuments 
     */
    val size = tokens.size

    tokens
      .foldLeft(Map.empty[String, Int])((map: Map[String, Int], token: String) => map + (token -> (map.getOrElse(token, 0) + 1)))
      .foldLeft(Map.empty[String, Double])((map, next) => map + (next._1 -> next._2.toDouble / size))
  }

  def computeSimilarity(record: ((String, String), (String, String)), idfDictionary: Map[String, Double], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     */
    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfDictionary, stopWords))
  }

  def calculateTF_IDF(terms: List[String], idfDictionary: Map[String, Double]): Map[String, Double] = {
    /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */
    val size = terms.length
    var tmp = terms
      .foldLeft(Map.empty[String, Int])((map, word) => map + (word -> (map.getOrElse(word, 0) + 1)))
    for (x <- tmp) yield x._1 -> ((x._2.toDouble / size) * idfDictionary.getOrElse(x._1, 0.toDouble))
  }

  def calculateDotProduct(v1: Map[String, Double], v2: Map[String, Double]): Double = {
    /*
     * Berechnung des Dot-Products von zwei Vectoren
     */
    v1.foldLeft(0.toDouble)((sum, next) => sum + (v2.getOrElse(next._1, 0.toDouble) * next._2))
  }

  def calculateNorm(vec: Map[String, Double]): Double = {
    /*
     * Berechnung der Norm eines Vectors
     */
    Math.sqrt((for (el <- vec.values) yield el * el).sum)
  }

  def calculateCosinusSimilarity(doc1: Map[String, Double], doc2: Map[String, Double]): Double = {
    /* 
     * Berechnung der Cosinus-Similarity für zwei Vectoren
     */
    val sum: Double = doc1.foldLeft(0.toDouble)((sum, element) => sum + element._2 * doc2.getOrElse(element._1, 0.toDouble))
    val scalar_a: Double = doc1.foldLeft(0.toDouble)((sum, element) => sum + element._2 * element._2)
    val scalar_b: Double = doc2.foldLeft(0.toDouble)((sum, element) => sum + element._2 * element._2)

    sum / (math.sqrt(scalar_a) * math.sqrt(scalar_b))
  }

  def calculateDocumentSimilarity(doc1: String, doc2: String, idfDictionary: Map[String, Double], stopWords: Set[String]): Double = {
    /*
     * Berechnung der Document-Similarity für ein Dokument
     */
    val doc_1 = tokenize(doc1, stopWords).map(word => word -> idfDictionary.getOrElse(word, 0.toDouble)).toMap
    val doc_2 = tokenize(doc2, stopWords).map(word => word -> idfDictionary.getOrElse(word, 0.toDouble)).toMap
    calculateCosinusSimilarity(doc_1, doc_2)
  }

  def computeSimilarityWithBroadcast(record: ((String, String), (String, String)), idfBroadcast: Broadcast[Map[String, Double]], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     * Verwenden Sie die Broadcast-Variable.
     */
    ???
  }
}
