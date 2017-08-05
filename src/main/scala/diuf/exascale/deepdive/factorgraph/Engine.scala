package diuf.exascale.deepdive.factorgraph

/**
  * Created by sam on 7/15/17.
  */

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import Inference._
import Materialization._
import org.apache.spark.sql.functions._

object Engine {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Deepdive Engine")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    //    inputs
    val hadoop_dir = args(0)
    val weights: Dataset[Weight] = clean_weights(hadoop_dir)
    val variables: Dataset[Variable] = clean_variables(hadoop_dir)
    val factors: Dataset[Factor] = clean_factors(hadoop_dir)
    val E: Dataset[Edge]= clean_edges(factors, variables)
    val A: Dataset[VariableAssignment]  = Sampler.random_assignment(variables)
    if(materialization_stat(factors, variables)){
      val q = compute_q(vcc(E),A)
      val q_grouped = compute_QGrouped(q)
      Sampler.gibbs(q_grouped, 10)
    }else {
      fcc(E, A)
    }
  } //-- main

  def materialization_stat(factors: Dataset[Factor], variable: Dataset[Variable]): Boolean={
    true //variables are way less than factors
//    if(factors.count() > variable.count() * 2)
//      true // QV
//    else
//      false //QF
  } //-- choose between V-CoC or F-COC
}
