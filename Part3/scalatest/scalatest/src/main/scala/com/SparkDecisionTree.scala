package com

import org.apache.spark._

import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.sql.SQLContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

object SparkDecisionTree {

  case class Patent(docnumber: String, inventiontitle: String, classification: String, year: Integer, ccount: Integer, orgname: String, word1: String, word2: String, word3: String)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    // function to parse input into Movie class  
    def parsePatent(str: String): Patent = {
      val line = str.split(",")
      Patent(line(0), line(1), line(2).substring(0, 3).toString(), line(3).toInt, line(4).toInt, line(5), line(6), line(7), line(8))
    }

    /* -------------------------------- MLLIB------------------------------------------ */
    val textRDD = sc.textFile(args(0))

    val patentsRDD = textRDD.map(parsePatent).cache()

    val patentsDF = patentsRDD.toDF()
    //Register as table
    patentsDF.registerTempTable("patents")

    var classMap: Map[String, Int] = Map()
    var index2: Int = 0
    patentsRDD.map(patent => patent.classification).distinct.collect.foreach(x => { classMap += (x -> index2); index2 += 1 })
    sc.parallelize(classMap.toSeq).saveAsTextFile("///home//Sriharish//classMap")

    /* var orgMap: Map[String, Int] = Map()
    var index3: Int = 0
    patentsRDD.map(patent => patent.orgname).distinct.collect.foreach(x => { orgMap += (x -> index3); index3 += 1 })
    sc.parallelize(orgMap.toSeq).saveAsTextFile("///home//Sriharish//orgMap")*/

    var word1Map: Map[String, Int] = Map()
    var index4: Int = 0
    patentsRDD.map(patent => patent.word1).distinct.collect.foreach(x => { word1Map += (x -> index4); index4 += 1 })
    sc.parallelize(word1Map.toSeq).saveAsTextFile("///home//Sriharish//word1Map")

    var word2Map: Map[String, Int] = Map()
    var index5: Int = 0
    patentsRDD.map(patent => patent.word2).distinct.collect.foreach(x => { word2Map += (x -> index5); index5 += 1 })
    sc.parallelize(word2Map.toSeq).saveAsTextFile("///home//Sriharish//word2Map")

    var word3Map: Map[String, Int] = Map()
    var index6: Int = 0
    patentsRDD.map(patent => patent.word3).distinct.collect.foreach(x => { word3Map += (x -> index6); index6 += 1 })
    sc.parallelize(word3Map.toSeq).saveAsTextFile("///home//Sriharish//word3Map")

    //- Defining the features array
    val mlprep = patentsRDD.map(patent => {
      val classification = classMap(patent.classification)
      //val orgname = orgMap(patent.orgname)
      val word1 = word1Map(patent.word1)
      val word2 = word2Map(patent.word2)
      val word3 = word3Map(patent.word3)
      Array(classification.toDouble, word1.toDouble, word2.toDouble, word3.toDouble)
    })
    //Making LabeledPoint of features - this is the training data for the model
    val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3))))

    val splits = mldata.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    var categoricalFeaturesInfo = Map[Int, Int]()
    categoricalFeaturesInfo += (0 -> word1Map.size)
    categoricalFeaturesInfo += (1 -> word2Map.size)
    categoricalFeaturesInfo += (2 -> word3Map.size)

    val numClasses = classMap.size
    // Defning values for the other parameters
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = word3Map.size

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()

    println("Learned Classification tree model:\n" + model.toDebugString)

    println("Test Error = " + testErr)
    
    model.save(sc, args(1))

  }
}
