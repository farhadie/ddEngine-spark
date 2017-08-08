package diuf.exascale.deepdive.factorgraph

/**
  * Created by sam on 7/15/17.
  */

import org.apache.spark.sql.{Dataset, SparkSession}
import Inference._
import Materialization._
import org.apache.spark.rdd.RDD

object Engine {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Deepdive Engine")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    val t0 = System.nanoTime()
    val iterations = args(1).toInt
    val burnout = args(2).toInt
    val thin = args(3).toInt
    //    inputs
    val hadoop_dir = "home/sam/thesis/gibbs/smoke/2v"//args(0)
    val weights: Dataset[Weight] = clean_weights(hadoop_dir)
    val variables: Dataset[Variable] = clean_variables(hadoop_dir)
    val factors: Dataset[Factor] = clean_factors(hadoop_dir, weights)
    val E: Dataset[Edge]= clean_edges(factors, variables)
    val A: RDD[(Long,Double)]  = Sampler.random_assignment(variables)

    if(materialization_stat(factors, variables)){
      val sample_worlds = Sampler.gibbs(vcc(E),A, iterations)
      //val output = infer(sample_worlds, factors, iterations, burnout, thin)
    }else {
    }
  val t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0) + "ns")
  } //-- main

  def materialization_stat(factors: Dataset[Factor], variable: Dataset[Variable]): Boolean={
    true //variables are way less than factors
//    if(factors.count() > variable.count() * 2)
//      true // QV
//    else
//      false //QF
  } //-- choose between V-CoC or F-COC
}
