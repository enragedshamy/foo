package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jfree.data.statistics.HistogramDataset
import org.jfree.data.statistics.HistogramType
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.JFreeChart
import org.jfree.chart.ChartFactory
import javax.swing.JFrame
import org.jfree.chart.ChartPanel
import javax.swing.WindowConstants
import textanalyse.EntityResolution
import textanalyse.Utils


object SimilarityApp extends App {
 
  val dat1="Amazon_small.csv"
  val dat2="Google_small.csv"
  val stopws="stopwords.txt"
  val goldStandardFile="Amazon_Google_perfectMapping.csv"
    
  val conf:SparkConf = new SparkConf().setMaster("local[4]").setAppName("Beleg3EntityResolutionApp")
  conf.set("spark.executor.memory","4g")
  conf.set("spark.storage.memoryFraction","0.8")
  conf.set("spark.driver.memory", "2g")
      
  val sc:SparkContext = new SparkContext(conf)
  val stopwords= Utils.getStopWords(stopws)
 
  val entityResolution= new EntityResolution(sc, dat1,dat2, stopws,goldStandardFile)
  val amazon = entityResolution.getTokens(Utils.getData(dat1, sc)).cache
  val google = entityResolution.getTokens(Utils.getData(dat2, sc)).cache
 
  val total= entityResolution.createCorpus
  entityResolution.calculateIDF
  val idfs= entityResolution.idfDict.values.toArray
  
  val dataset:HistogramDataset = new HistogramDataset
  dataset.setType(HistogramType.RELATIVE_FREQUENCY)
  dataset.addSeries("Histogram",idfs,50)
  val plotTitle:String= "Histogramm" 
  val xaxis:String  = "idf-Wert"
  val yaxis:String = "Häufigkeit des idf-Werts"
  val orientation:PlotOrientation = PlotOrientation.VERTICAL 
  val show:Boolean  = false
  val toolTips:Boolean= false
  val urls:Boolean= false
  val chart:JFreeChart= ChartFactory.createHistogram( plotTitle, xaxis, yaxis, 
                dataset, orientation, show, toolTips, urls)
  
  val frame:JFrame = new JFrame("Histogramm: Relative Häufigkeiten der IDF-Werte") 
  frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)
  val chartPanel: ChartPanel = new ChartPanel(chart)
  frame.setContentPane(chartPanel)
  frame.pack
  frame.setVisible(true)
  
  println("Please press enter....")
  System.in.read()
  frame.setVisible(false)
  frame.dispose
  if (sc!=null) {sc.stop; println("Spark stopped......")}
  
}