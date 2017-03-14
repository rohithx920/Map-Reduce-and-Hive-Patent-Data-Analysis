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
import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }

object KMeansCluster {

  case class Patent(docnumber: String, inventiontitle: String, classification: String, year: Integer, ccount: Integer, orgname: String, word1: String, word2: String, word3: String)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkKmeans").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    // function to parse input into Movie class  
    def parsePatent(str: String): Patent = {
      val line = str.split(",")
      Patent(line(0), line(1), line(2).substring(0, 3), line(3).toInt, line(4).toInt, line(5), line(6), line(7), line(8))
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

    var word1Map: Map[String, Int] = Map()
    var index4: Int = 0
    patentsRDD.map(patent => patent.word1).distinct.collect.foreach(x => { word1Map += (x -> index4); index4 += 1 })

    var word2Map: Map[String, Int] = Map()
    var index5: Int = 0
    patentsRDD.map(patent => patent.word2).distinct.collect.foreach(x => { word2Map += (x -> index5); index5 += 1 })

    var word3Map: Map[String, Int] = Map()
    var index6: Int = 0
    patentsRDD.map(patent => patent.word3).distinct.collect.foreach(x => { word3Map += (x -> index6); index6 += 1 })

    //- Defining the features array
    val mlprep = patentsRDD.map(patent => {
      val classification = classMap(patent.classification)
      val word1 = word1Map(patent.word1)
      val word2 = word2Map(patent.word2)
      val word3 = word3Map(patent.word3)
      Array(classification.toDouble, word1.toDouble, word2.toDouble, word3.toDouble)
    })
    //Making LabeledPoint of features - this is the training data for the model
    val mldata = mlprep.map(x => Vectors.dense(x(0), x(1), x(2), x(3)))

    // Cluster the data into two classes using KMeans
    val numClusters = 12
    val numIterations = 10
    val clusters = KMeans.train(mldata, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(mldata)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
    clusters.save(sc, args(1))
    val sameModel = KMeansModel.load(sc, args(1))
    // $example off$
    sameModel.clusterCenters.foreach { x => print(x) }
    sc.stop()
  }

}