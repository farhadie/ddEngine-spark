package diuf.exascale.deepdive.factorgraph

/**
  * Created by sam on 7/15/17.
  */

import org.apache.spark.sql.SparkSession
import Inference._
import Grounding._
import org.apache.spark.rdd.RDD

object Engine {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Deepdive Engine")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    //    inputs
    val hadoop_dir = args(0)
    val weights: RDD[Weight] = clean_weights(hadoop_dir)
    val variables: RDD[Variable] = clean_variables(hadoop_dir)
    val factors: RDD[Factor] = clean_factors(hadoop_dir)
    val graph = Graph(variables, factors, weights)

    gibbs_sampler(graph,args(1).toInt,2,2)
  } //-- main
}
