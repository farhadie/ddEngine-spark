package diuf.exascale.deepdive.factorgraph

import org.apache.spark.rdd.RDD

/**
  * learning weights of factor graph
  * Created by Ehsan.
  */
object Learning {
  def sgd()={}
  def learn_weights(factors: RDD[Factor], variables: RDD[Variable], weights: RDD[Weight]): RDD[Factor] = {
    factors.map{
      r => {
        r.set_weight(1)
        r
      }
    }
  }
}
