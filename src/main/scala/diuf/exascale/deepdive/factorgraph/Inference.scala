package diuf.exascale.deepdive.factorgraph

/**
  * Created by Ehsan.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.Map

object Inference {
  import Engine.spark.implicits._
  def infer(sample_worlds: DataFrame, factors: Dataset[Factor], iterations: Int, burnout: Int, thin: Int): Unit = {
    var list_of_pr = Array.empty[Double]
    var sum = 0.0
    for (i <- burnout until iterations by thin) {
      val col = "sw" + i
      val values = sample_worlds.select("variable_id", col).rdd.map(r => (r.getLong(0),r.getDouble(1)))
      val probability = pr_I(factors,values)
      list_of_pr  = list_of_pr :+ probability
      sum += probability
    }
    val results = sample_worlds.map {
      variable => {
        var prob_v = 0.0
        var j = 0
        for(i<- burnout until iterations by thin){
          prob_v += variable.getDouble(i)*list_of_pr(j)/sum
          j += 1
        }
        (variable.getLong(0), prob_v)
      }
    }
    results.show
  }

  def pr_I(factor: Dataset[Factor], values: RDD[(Long, Double)]): Double = {
    val values_map = values.collectAsMap
    val factors_outcome = factor.map {
      f => {
        factor_evaluate(f.func,f.edge_count,f.variables,values_map)*f.weight
      }
    }.collect
    var outcome = 0.0
    factors_outcome.foreach{
      o => outcome = outcome + o
    }
    outcome
  }

  def factor_evaluate(func: Short, edges:Long, function_arguments: Array[(Long, Boolean)], A: Map[Long, Double]): Double = {
    func match {
      case 0 => //implication
        var a = true
        var b = true
        if(edges == 1){
          a = true
          b = if (A.getOrElse(function_arguments(0)._1.toLong, Double).asInstanceOf[Double] == 1) true else false
        }else{
          a = if (A.getOrElse(function_arguments(0)._1.toLong, Double).asInstanceOf[Double] == 1) true else false
          b = if (A.getOrElse(function_arguments(1)._1.toLong, Double).asInstanceOf[Double] == 1) true else false
        }
        if (!a || b) 1 else 0
    }
  }
}
