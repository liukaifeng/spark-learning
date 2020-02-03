//package com.lkf.pmml
//
//
//import java.io.{File, FileInputStream, InputStream}
//
//import org.jpmml.evaluator.spark.TransformerBuilder
//import java.util.Arrays
//
//import org.apache.spark.SparkConf
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
//import org.apache.spark.sql.SparkSession
//import org.dmg.pmml.PMML
//import org.jpmml.evaluator.LoadingModelEvaluatorBuilder
//import org.jpmml.model.PMMLUtil
//// Helper object for case class for Spark
//object IrisHelper {
//  case class InputRecord(Sepal_Length: Double, Sepal_Width: Double, Petal_Length: Double, Petal_Width: Double)
//  case class DefaultResultRecord(Species: String, Probability_setosa: Double, Probability_versicolor: Double, Probability_virginica: Double, Node_id: String)
//  case class CustomResultRecord(label: String, probabilities: Vector)
//}
//
//class PMMLTransformerTest  {
//  import IrisHelper._
//
//  def main(args: Array[String]): Unit = {
//    implicit val sparkSession = SparkSession
//      .builder()
//      .config(
//        new SparkConf()
//          .setAppName("DecisionTreeIris")
//          .setMaster("local")
//      ).getOrCreate()
//
//    val inputRdd = sparkSession.sparkContext.makeRDD(Seq(
//      InputRecord(5.1, 3.5, 1.4, 0.2),
//      InputRecord(7, 3.2, 4.7, 1.4),
//      InputRecord(6.3, 3.3, 6, 2.5)
//    ))
//    val inputDs = sparkSession.createDataFrame(inputRdd)
//
//    val expectedDefaultResultRdd = sparkSession.sparkContext.makeRDD(Seq(
//      DefaultResultRecord("setosa", 1.0, 0.0, 0.0, "2"),
//      DefaultResultRecord("versicolor", 0.0, 0.9074074074074074, 0.09259259259259259, "6"),
//      DefaultResultRecord("virginica", 0.0, 0.021739130434782608, 0.9782608695652174, "7")
//    ))
//    val expectedDefaultResultDs = sparkSession.createDataFrame(expectedDefaultResultRdd)
//
//    val expectedCustomResultRdd = sparkSession.sparkContext.makeRDD(Seq(
//      CustomResultRecord("setosa", Vectors.dense(1.0, 0.0, 0.0)),
//      CustomResultRecord("versicolor", Vectors.dense(0.0, 0.9074074074074074, 0.09259259259259259)),
//      CustomResultRecord("virginica", Vectors.dense(0.0, 0.021739130434782608, 0.9782608695652174))
//    ))
//    val expectedCustomResultDs = sparkSession.createDataFrame(expectedCustomResultRdd)
//
//    // Load the PMML
//    val pmmlIs = getClass.getClassLoader.getResourceAsStream("DecisionTreeIris.pmml")
//
//    // Create the evaluator
//    val evaluator = new LoadingModelEvaluatorBuilder()
//      .load(pmmlIs)
//      .build()
//
//    // Create the transformer
//    var pmmlTransformer = new TransformerBuilder(evaluator)
//      .withTargetCols
//      .withOutputCols
//      .exploded(true)
//      .build()
//
//    // Verify the transformed results
//    var resultDs = pmmlTransformer.transform(inputDs)
//    resultDs.show
//
//    resultDs = resultDs.select("Species", "Probability_setosa", "Probability_versicolor", "Probability_virginica", "Node_Id")
//
//    assert(resultDs.rdd.collect.toList == expectedDefaultResultDs.rdd.collect.toList)
//
//    pmmlTransformer = new TransformerBuilder(evaluator)
//      .withLabelCol("label")
//      .withProbabilityCol("probability", Arrays.asList("setosa", "versicolor", "virginica"))
//      .exploded(true)
//      .build()
//
//    resultDs = pmmlTransformer.transform(inputDs)
//    resultDs.show
//
//    resultDs = resultDs.select("label", "probability")
//
//    assert(resultDs.rdd.collect.toList == expectedCustomResultDs.rdd.collect.toList)
//
//  }
//
//
//  @throws[Exception]
//  def readPMML(file: File): PMML = {
//    val is = new FileInputStream(file)
//    PMMLUtil.unmarshal(is)
//  }
//}