================== ITCS-6190-Cloud Computing for Data Analysis1 ====================
================== Group 5 - Project Part 1 ========================================

Part 1:
================== Description =====================================================
* Java project created and executed using eclipse and Maven in Cloudera VM
* XMLDriver class has the main method to run the job
* XmlInputFormat class has the logic to parse the XML files.
* MyMapper class outputs the value of required elements/tags from the xml file
* The MyReducer class outputs the patentDataAttribute file
* The source code and executable jar file is provided.
* The jar can be executed with below command.
  hadoop jar xmlParsingMapreduce.jar  com.ccproject.xmlParsing.XMLDriver  <input files directory> <output directory>
=================== Comments ========================================================
* The map reduce program executes succeefully generating patentData.txt and patentDataAttributes.txt files.
              
=====================================================================================


Part 2:

=====================================================================================
Table Creation in Hive
Query:  hadoop fs -put ./projectPart2/patentdata.txt  /user/lsridhar/projectPart2_input/ 
CREATE TABLE IF NOT EXISTS Group5_PatentData (doc_number INT, invention_title STRING, classification_national STRING, year INT, org_name STRING, word1 STRING, word2 STRING, word3 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA INPATH '/user/lsridhar/projectPart2_input/patentData.txt' INTO TABLE Group5_PatentData;

2.1. All Patents from year 2015
 
Query: INSERT OVERWRITE DIRECTORY 'patentdata_output/query1' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT * FROM Group5_PatentData where year = 2015;

2.2. All Patents owned by the company ' Apple '
Query: INSERT OVERWRITE DIRECTORY 'patentdata_output/query2' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT * FROM Group5_PatentData WHERE LOWER(org_name) LIKE '%apple %';

2.3. All patents in class electronics, that contain the frequent word ' Samsung ' AND ' Apple '
Query: INSERT OVERWRITE DIRECTORY 'patentdata_output/query3' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE SELECT COUNT(*) FROM Group5_PatentData WHERE LOWER(classification_national) LIKE '%electronic%' AND (((LOWER(word1) LIKE '%samsung%' OR LOWER(word1) LIKE '%apple%') AND (LOWER(word2) LIKE '%samsung%' OR LOWER(word2) LIKE '%apple%')) OR ((LOWER(word2) LIKE '%samsung%' OR LOWER(word2) LIKE '%apple%') AND (LOWER(word3) LIKE '%samsung%' OR LOWER(word3) LIKE '%apple%')) OR ((LOWER(word1) LIKE '%samsung%' OR LOWER(word1) LIKE '%apple%') AND (LOWER(word3) LIKE '%samsung%' OR LOWER(word3) LIKE '%apple%')));

========================================================================================
========================================================================================
part3:

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
              

