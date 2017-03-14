================== ITCS-6190-Cloud Computing for Data Analysis1 ====================
================== Group 5 - Project Part 3 ========================================

================== Description =====================================================
* Java project created and executed using eclipse and Maven in Cloudera VM
* Decision Tree, uses the attribute PatentClass as a decision attribute
  spark-submit --class com.SparkDecisionTree --master local[2] /home/cloudera/scalatest-1.0-SNAPSHOT-jar-with-dependencies.jar 
  /home/cloudera/patentData.txt /home/cloudera/DecisionTreeOutput >> decisiontreeoutput.txt
* Classification, uses the attribute word1 ( most frequent word ) as a decision attribute.
  spark-submit --class com.SparkPatentClassification --master local[2] /home/cloudera/scalatest-1.0-SNAPSHOT-jar-with-dependencies.jar 
  /home/cloudera/patentData.txt /home/cloudera/ClassificationOutput >> classificationoutput.txt
* Clustering- uses K-MEANS clustering , creates 12 clusters ; - use hierarchical clustering ( number of clusters automatic ) 
  spark-submit --class com.Hierarchical --master local[2] /home/cloudera/scalatest-1.0-SNAPSHOT-jar-with-dependencies.jar 
  /home/cloudera/patentData.txt /home/cloudera/HierarchicalOutput >> hierarchicaloutput.txt
  spark-submit --class com.KMeansCluster --master local[2] /home/cloudera/scalatest-1.0-SNAPSHOT-jar-with-dependencies.jar
 /home/cloudera/patentData.txt /home/cloudera/KMeansOutput >> kmeansoutput.txt

=================== Comments ========================================================
* The jar is executed sucessfully generating the corresponding outputs.
              
=====================================================================================
