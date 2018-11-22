package textanalyse

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYSplineRenderer
import org.jfree.chart.{ChartPanel, JFreeChart}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import org.jfree.ui.ApplicationFrame
import textanalyse.EntityResolution.getTermFrequencies


class ScalableEntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String, entityResolution: EntityResolution) extends EntityResolution(sc, dat1, dat2, stopwordsFile, goldStandardFile) {

  // Creation of the tf-idf-Dictionaries
  createCorpus
  calculateIDF
  val idfsFullBroadcast = sc.broadcast(idfDict)

  // Preparation of all Document Vectors
  def calculateDocumentVector(productTokens: RDD[(String, List[String])], idfDictBroad: Broadcast[Map[String, Double]]): RDD[(String, Map[String, Double])] = {

    productTokens.map(x => (x._1, ScalableEntityResolution.calculateTF_IDFBroadcast(x._2, idfDictBroad)))
  }

  val amazonWeightsRDD: RDD[(String, Map[String, Double])] = calculateDocumentVector(entityResolution.amazonTokens, idfsFullBroadcast)
  val googleWeightsRDD: RDD[(String, Map[String, Double])] = calculateDocumentVector(entityResolution.googleTokens, idfsFullBroadcast)

  // Calculation of the L2-Norms for each Vector
  val amazonNorms = amazonWeightsRDD.map(x => (x._1, EntityResolution.calculateNorm(x._2))).collectAsMap().toMap
  val amazonNormsBroadcast = sc.broadcast(amazonNorms)
  val googleNorms = googleWeightsRDD.map(x => (x._1, EntityResolution.calculateNorm(x._2))).collectAsMap().toMap
  val googleNormsBroadcast = sc.broadcast(googleNorms)


  val BINS = 101
  val nthresholds = 100
  val zeros: Vector[Int] = Vector.fill(BINS) {
    0
  }
  val thresholds = for (i <- 1 to nthresholds) yield i / nthresholds.toDouble
  var falseposDict: Map[Double, Long] = _
  var falsenegDict: Map[Double, Long] = _
  var trueposDict: Map[Double, Long] = _

  var fpCounts = sc.accumulator(zeros)(VectorAccumulatorParam)

  var amazonInvPairsRDD: RDD[(String, String)] = _
  var googleInvPairsRDD: RDD[(String, String)] = _

  var commonTokens: RDD[((String, String), Iterable[String])] = _
  var similaritiesFullRDD: RDD[((String, String), Double)] = _
  var simsFullValuesRDD: RDD[Double] = _
  var trueDupSimsRDD: RDD[Double] = _


  var amazonWeightsBroadcast: Broadcast[Map[String, Map[String, Double]]] = _
  var googleWeightsBroadcast: Broadcast[Map[String, Map[String, Double]]] = _
  this.amazonWeightsBroadcast = amazonWeightsRDD.sparkContext.broadcast(amazonWeightsRDD.collectAsMap().toMap)
  this.googleWeightsBroadcast = amazonWeightsRDD.sparkContext.broadcast(googleWeightsRDD.collectAsMap().toMap)

  def buildInverseIndex: Unit = {

    /*
     * Aufbau eines inversen Index
     * Die Funktion soll die Variablen amazonWeightsRDD und googleWeightsRDD so
     * umwandeln, dass aus dem EingabeRDD vom Typ  RDD[(String, Map[String,Double])]
     * alle Tupel der Form (Wort, ProduktID) extrahiert werden.
     * Verwenden Sie dazu die Funktion invert im object und speichern Sie die
     * Ergebnisse in amazonInvPairsRDD und googleInvPairsRDD. Cachen Sie die
     * die Werte.
     */

    // data:
    // amazonWeightsRDD: Tuple[]: 0: (b000jz4hqo,Map(premier -> 9.27070707070707, broderbund -> 22.169082125603865, image -> 3.6948470209339774, pack -> 2.98180636777128, rom -> 2.4051362683438153, dvd -> 1.287598204264871, 950 -> 254.94444444444443, 000 -> 6.218157181571815, clickart -> 56.65432098765432)), 1: (b0006zf55o,Map(desktop -> 2.23635477582846, win -> 0.501859142607174, backup -> 2.8015873015873014, arcserve -> 24.28042328042328, 30pk -> 254.94444444444443, oem -> 46.35353535353535, ca -> 9.10515873015873, laptops -> 11.588383838383837, computer -> 0.6965695203400122, 1 -> 0.3231235037318687, international -> 9.44238683127572, desktops -> 12.74722222222222, v11 -> 50.98888888888888, lap -> 127.47222222222221, 30u -> 84.98148148148148, associates -> 7.284126984126985))
    // googleWeightsRDD: Tuple[]: 0: (http://www.google.com/base/feeds/snippets/11125907881740407428,Map(2007 -> 4.985334057577403, quickbooks -> 17.48190476190476, learning -> 5.932773109243698, intuit -> 13.379008746355684)), 1: (http://www.google.com/base/feeds/snippets/11538923464407758599,Map(writing -> 9.695070422535212, read -> 6.373611111111112, learn -> 1.1950520833333333, superstart -> 57.362500000000004, designed -> 0.8466789667896679, reading -> 6.286301369863014, creative -> 1.130295566502463, puzzle -> 4.98804347826087, help -> 0.71703125, exercises -> 4.881914893617022, fun -> 2.2495098039215686, kids -> 2.6073863636363637, solving -> 9.97608695652174, better -> 1.5195364238410596, decoding -> 114.72500000000001, write -> 3.8241666666666667))

    // function to use: def invert(termlist:(String, Map[String,Double])):List[(String,String)]

    amazonInvPairsRDD = amazonWeightsRDD.map(x => ScalableEntityResolution.invert(x)).flatMap(list => list).cache() // Tuple[] : ("000","b000jz4hqo"), ("premier","b000jz4hqo"),
    googleInvPairsRDD = googleWeightsRDD.map(x => ScalableEntityResolution.invert(x)).flatMap(list => list).cache() // Tuple[]: ("2007","http://www.google.com/base/feeds/snippets/11125907881740407428"), ("quickbooks","http://www.google.com/base/feeds/snippets/11125907881740407428")
  }

  def determineCommonTokens: Unit = {

    /*
     * Bestimmen Sie alle Produktkombinationen, die gemeinsame Tokens besitzen
     * Speichern Sie das Ergebnis in die Variable commonTokens und verwenden Sie
     * dazu die Funktion swap aus dem object.
     */

    // result type (commonTokens): RDD[((String, String), scala.Iterable[String])]
    // function to use: def swap(el:(String,(String,String))):((String, String),String)

    // USE amazon and google PAIRS
    // Tuple[] : ("000","b000jz4hqo"), ("premier","b000jz4hqo"),
    // Tuple[]: ("2007","http://www.google.com/base/feeds/snippets/11125907881740407428"), ("quickbooks","http://www.google.com/base/feeds/snippets/11125907881740407428")

    val temp = amazonInvPairsRDD.join(googleInvPairsRDD) // 0: (vga,(b000099sin,http://www.google.com/base/feeds/snippets/12536674159579454939)), 1: (vga,(b000099sin,http://www.google.com/base/feeds/snippets/2930329403993141037))
    val temp1 = temp.map(x => x.swap) // [((String, String), String)]
    commonTokens = temp1.groupByKey()
  }

  def calculateSimilaritiesFullDataset: Unit = {

    /*
     * Berechnung der Similarity Werte des gesmamten Datasets
     * Verwenden Sie dafür das commonTokensRDD (es muss also mind. ein
     * gleiches Wort vorkommen, damit der Wert berechnent dafür.
     * Benutzen Sie außerdem die Broadcast-Variablen für die L2-Norms sowie
     * die TF-IDF-Werte.
     *
     * Für die Berechnung der Cosinus-Similarity verwenden Sie die Funktion
     * fastCosinusSimilarity im object
     * Speichern Sie das Ergebnis in der Variable simsFillValuesRDD und cachen sie diese.
     */

    val _amazonWeightsBroadcastRDD = amazonWeightsBroadcast
    val _googleWeightsBroadcastRDD = googleWeightsBroadcast
    val _amazonNormsBroadcast = amazonNormsBroadcast
    val _googleNormsBroadcast = googleNormsBroadcast

    val _commonsTokens = commonTokens

    commonTokens.map(x => ScalableEntityResolution.fastCosinusSimilarity(x, _amazonWeightsBroadcastRDD, _googleWeightsBroadcastRDD, _amazonNormsBroadcast, _amazonNormsBroadcast))
  }

  /*
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   *
   * Analyse des gesamten Datensatzes mittels des Gold-Standards
   *
   * Berechnung:
   * True-Positive
   * False_Positive
   * True-Negative
   * False-Negative
   *
   * und daraus
   * Precision
   * Recall
   * F-Measure
   *
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   */

  def analyseDataset: Unit = {


    val simsFullRDD = similaritiesFullRDD.map(x => (x._1._1 + " " + x._1._2, x._2)).cache
    simsFullRDD.take(10).foreach(println)
    goldStandard.take(10).foreach(println)
    val tds = goldStandard.leftOuterJoin(simsFullRDD)
    tds.filter(x => x._2._2 == None).take(100).foreach(println)
    trueDupSimsRDD = goldStandard.leftOuterJoin(simsFullRDD).map(ScalableEntityResolution.gs_value(_)).cache()


    def calculateFpCounts(fpCounts: Accumulator[Vector[Int]]): Accumulator[Vector[Int]] = {

      val BINS = this.BINS
      val nthresholds = this.nthresholds
      val fpCounts_ : Accumulator[Vector[Int]] = fpCounts
      simsFullValuesRDD.foreach(ScalableEntityResolution.add_element(, BINS, nthresholds, fpCounts))
      trueDupSimsRDD.foreach(ScalableEntityResolution.sub_element(, BINS, nthresholds, fpCounts))
      fpCounts_
    }

    fpCounts = calculateFpCounts(fpCounts)
    falseposDict = (for (t <- thresholds) yield (t, falsepos(t, fpCounts))).toMap
    falsenegDict = (for (t <- thresholds) yield (t, falseneg(t))).toMap
    trueposDict = (for (t <- thresholds) yield (t, truepos(t))).toMap

    val precisions = for (t <- thresholds) yield (t, precision(t))
    val recalls = for (t <- thresholds) yield (t, recall(t))
    val fmeasures = for (t <- thresholds) yield (t, fmeasure(t))

    val series1: XYSeries = new XYSeries("Precision");
    for (el <- precisions) {
      series1.add(el._1, el._2)
    }
    val series2: XYSeries = new XYSeries("Recall");
    for (el <- recalls) {
      series2.add(el._1, el._2)
    }
    val series3: XYSeries = new XYSeries("F-Measure");
    for (el <- fmeasures) {
      series3.add(el._1, el._2)
    }

    val datasetColl: XYSeriesCollection = new XYSeriesCollection
    datasetColl.addSeries(series1)
    datasetColl.addSeries(series2)
    datasetColl.addSeries(series3)

    val spline: XYSplineRenderer = new XYSplineRenderer();
    spline.setPrecision(10);

    val xax: NumberAxis = new NumberAxis("Similarities");
    val yax: NumberAxis = new NumberAxis("Precision/Recall/F-Measure");

    val plot: XYPlot = new XYPlot(datasetColl, xax, yax, spline);

    val chart: JFreeChart = new JFreeChart(plot);
    val frame: ApplicationFrame = new ApplicationFrame("Dataset Analysis");
    val chartPanel1: ChartPanel = new ChartPanel(chart);

    frame.setContentPane(chartPanel1);
    frame.pack();
    frame.setVisible(true);
    println("Please press enter....")
    System.in.read()
  }


  /*
   * Berechnung von False-Positives, FalseNegatives und
   * True-Positives
   */
  def falsepos(threshold: Double, fpCounts: Accumulator[Vector[Int]]): Long = {
    val fpList = fpCounts.value
    (for (b <- Range(0, BINS) if b.toDouble / nthresholds >= threshold) yield fpList(b)).sum
  }

  def falseneg(threshold: Double): Long = {

    trueDupSimsRDD.filter(_ < threshold).count()
  }

  def truepos(threshold: Double): Long = {

    trueDupSimsRDD.count() - falsenegDict(threshold)
  }

  /*
   *
   * Precision = true-positives / (true-positives + false-positives)
   * Recall = true-positives / (true-positives + false-negatives)
   * F-measure = 2 x Recall x Precision / (Recall + Precision)
   */


  def precision(threshold: Double): Double = {
    val tp = trueposDict(threshold)
    tp.toDouble / (tp + falseposDict(threshold))
  }

  def recall(threshold: Double): Double = {
    val tp = trueposDict(threshold)
    tp.toDouble / (tp + falsenegDict(threshold))
  }

  def fmeasure(threshold: Double): Double = {
    val r = recall(threshold)
    val p = precision(threshold)
    2 * r * p / (r + p)
  }
}

object ScalableEntityResolution {

  def calculateTF_IDFBroadcast(terms: List[String], idfDictBroadcast: Broadcast[Map[String, Double]]): Map[String, Double] = {

    /*
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Map die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */
    val tf = getTermFrequencies(terms).toList // List: (customizing,0.16666666666666666), (2007,0.16666666666666666), ...
    val tfwords = tf.map(x => x._1).toList // List: "customizing", "2007", ...
    // idfDictionary: HashMap: (serious,400.0), (boutiques,400.0), (breaks,400.0)

    val temp = idfDictBroadcast.value.map(x => if (tfwords.contains(x._1)) (x._1, x._2 * tf.find(z => z._1 == x._1).get._2) else (x._1, x._2 * 0)) // HashMap: (customizing,16.666666666666664), (2007,3.5087719298245617), ...
    val result = temp.filter(_._2 != 0)
    result // HashMap: (customizing,16.666666666666664), (2007,3.5087719298245617), (interface,3.0303030303030303)...
  }

  def invert(termlist: (String, Map[String, Double])): List[(String, String)] = {

    //in: List of (ID, tokenList with TFIDF-value)
    //out: List[(token,ID)]

    termlist._2.keys.map(x => (x, termlist._1)).toList
  }

  def swap(el: (String, (String, String))): ((String, String), String) = {

    /*
     * Wandelt das Format eines Elements für die Anwendung der
     * RDD-Operationen.
     */

    (el._2, el._1)
  }

  def fastCosinusSimilarity(record: ((String, String), Iterable[String]),
                            amazonWeightsBroad: Broadcast[Map[String, Map[String, Double]]], googleWeightsBroad: Broadcast[Map[String, Map[String, Double]]],
                            amazonNormsBroad: Broadcast[Map[String, Double]], googleNormsBroad: Broadcast[Map[String, Double]]): ((String, String), Double) = {

    /* Compute Cosine Similarity using Broadcast variables
    Args:
        record: ((ID, URL), token)
    Returns:
        pair: ((ID, URL), cosine similarity value)

    Verwenden Sie die Broadcast-Variablen und verwenden Sie für ein schnelles dot-Product nur die TF-IDF-Werte,
    die auch in der gemeinsamen Token-Liste sind
    */

    val dot = EntityResolution.calculateDotProduct(_: Map[String, Double], _: Map[String, Double])
    val aID = record._1._1
    val gID = record._1._2
    val dotProduct = dot(amazonWeightsBroad.value(aID), googleWeightsBroad.value(gID))
    val normMult = amazonNormsBroad.value(aID) * googleNormsBroad.value(gID)

    ((aID, gID), dotProduct / normMult)
  }

  def gs_value(record: (, (

  , Option[Double]
  ) ) ): Double = {

    record._2._2 match {
      case Some(d: Double) => d
      case None => 0.0
    }
  }

  def set_bit(x: Int, value: Int, length: Int): Vector[Int] = {

    Vector.tabulate(length) { i => {
      if (i == x) value else 0
    }
    }
  }

  def bin(similarity: Double, nthresholds: Int): Int = (similarity * nthresholds).toInt

  def add_element(score: Double, BINS: Int, nthresholds: Int, fpCounts: Accumulator[Vector[Int]]): Unit = {
    val b = bin(score, nthresholds)
    fpCounts += set_bit(b, 1, BINS)
  }

  def sub_element(score: Double, BINS: Int, nthresholds: Int, fpCounts: Accumulator[Vector[Int]]): Unit = {
    val b = bin(score, nthresholds)
    fpCounts += set_bit(b, -1, BINS)
  }
}