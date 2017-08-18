package diuf.exascale.deepdive.factorgraph

import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.Map
/**
  * learning weights of factor graph
  * Created by Ehsan.
  */
object Learning {
  def sgd(weight_id:Long, weights: Dataset[Weight], values: Map[Long, Array[Double]]): Double={
    val w_values = values.getOrElse(weight_id, Array).asInstanceOf[Array[Double]]
    var new_w:Double = 0 //initial
    val n = 0.1
    w_values.foreach{
      w =>  {
        new_w = new_w - (n * w)
      }
    }
    new_w
  }
  def learn_weights(sample_worlds: DataFrame, factors: Dataset[Factor],  weights: Dataset[Weight], iterations: Int):Dataset[Factor] = {
    val new_sw:Map[Long, Array[Double]] = sample_worlds.map{
      r =>{
        var v = Array.empty[Double]
        for(i<- 0 until iterations) v = v :+ r.getDouble(i)
        (r.getLong(0), v)
      }
    }.rdd.collectAsMap()
    factors.map{
      r => {
        val new_weight:Double = sgd(r.weight_id, weights, new_sw)
        Factor(r.id, r.weight_id, r.func, r.edge_count, r.variables, new_weight)
      }
    }
  }
}
