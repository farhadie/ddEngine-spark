package diuf.exascale.deepdive.factorgraph

/**
  * Created by Ehsan.
  */

import org.apache.spark.sql.{DataFrame, Dataset}
import scala.collection.Map

object Inference {
  import Engine.spark.implicits._
  def infer(sample_worlds: DataFrame, factors: Dataset[Factor], iterations: Int, burnout: Int, thin: Int): Unit = {
    var sum = 0.0
    val new_sw:Map[Long, Array[Double]] = sample_worlds.map{
      r =>{
        var v = Array.empty[Double]
        for(i<- burnout until iterations by thin) v = v :+ r.getDouble(i)
        (r.getLong(0), v)
      }
    }.rdd.collectAsMap()
    val probabilities:Array[Double] = pr_I(factors, new_sw)

    probabilities.foreach{
      p => sum +=p
    }

    val results = sample_worlds.map {
      variable => {
        var prob_v = 0.0
        var j = 0
        for(i<- burnout until iterations by thin){
          prob_v += variable.getDouble(i)*probabilities(j)/sum
          j += 1
        }
        (variable.getLong(0), prob_v)
      }
    }
    results.show
  }

  def pr_I(factor: Dataset[Factor], values: Map[Long, Array[Double]]): Array[Double] = {
    val num_sw = values.getOrElse(0, Array).asInstanceOf[Array[Double]].length
    println(num_sw)
    val factors_outcome = factor.map {
      f => {
        factor_evaluate(f.func,f.edge_count,f.variables,values).map(x => x*f.weight) //zarb vazn to hame
      }
    }.collect
    var result = Array.fill(num_sw){0.0}
    factors_outcome.foreach{
      o => {
        for(i <- 0 until num_sw){
          result(i) = result(i) + o(i)
        }
      }
    }
    result
  }

  def factor_evaluate(func: Short, edges:Long, function_arguments: Array[(Long, Boolean)], A: Map[Long, Array[Double]]): Array[Double] = {
    val num_sw = A.getOrElse(0, Array).asInstanceOf[Array[Double]].length
    println(A.getOrElse(0, Array).asInstanceOf[Array[Double]])
    var result = Array.empty[Double]
    func match {
      case 0 => //implication
        for(i <- 0 until num_sw){
          var a = true
          var b = true
          if(edges == 1){
            a = true
            b = if (A.getOrElse(function_arguments(0)._1.toLong, Array).asInstanceOf[Array[Double]](i) == 1) true else false
          }else{
            a = if (A.getOrElse(function_arguments(0)._1.toLong, Array).asInstanceOf[Array[Double]](i) == 1) true else false
            b = if (A.getOrElse(function_arguments(1)._1.toLong, Array).asInstanceOf[Array[Double]](i) == 1) true else false
          }
          result = result :+ (if (!a || b) 1.0 else 0.0)
        }
       result
    }
  }
}
