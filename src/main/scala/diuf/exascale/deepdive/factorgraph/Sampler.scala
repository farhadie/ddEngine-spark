package diuf.exascale.deepdive.factorgraph


/**
  * Created by sam on 8/3/17.
  */
import diuf.exascale.deepdive.factorgraph.Materialization.compute_QGrouped
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.Map
import scala.util.Random
object Sampler {
  import Engine.spark
  import Engine.spark.implicits._
  def random_assignment(variables: Dataset[Variable]): RDD[(Long, Double)] = {
    variables.map {
      r => {
        if (!r.isEvidence) {
          val rnd = new scala.util.Random
          (r.id, rnd.nextInt(r.cardinality.toInt).toDouble)
        }
        else (r.id, r.variable_initial_value)
      }
    }.rdd//maybe no need for this case class?
  }
  def factor_evaluate(variable_id:Long, variable_value:Double, func:Short, function_arguments:Array[(Long, Boolean)], A:Map[Long,Double]):Double = {
    func match {
     case 0 => //implication
       var a = false
       var b = false
       if(function_arguments.length == 2) {
         if (function_arguments(0)._1 == variable_id) {
           a = if (variable_value == 1) true else false
           b = if (A.getOrElse(function_arguments(1)._1.toLong, Double).asInstanceOf[Double] == 1) true else false
         } else {
           b = if (variable_value == 1) true else false
           a = if (A.getOrElse(function_arguments(0)._1.toLong, Double).asInstanceOf[Double] == 1) true else false
         }
       }else if(function_arguments.length == 1){
         b = if (variable_value == 1) true else false
         a = if (A.getOrElse(function_arguments(0)._1.toLong, Double).asInstanceOf[Double] == 1) true else false
       }

       if (!a || b) 1 else 0
    }
  }
  def sample(variable_id:Long, factors:Array[(Long,Short,Double,Long,Array[(Long, Boolean)])], AMap: Map[Long, Double]):Double = {
    var i = 0
    var new_value_0:Double = 1
    var new_value_1:Double = 1
    var new_A = AMap
    while (i<factors.length){
      //for boolean
      new_value_0 = new_value_0 * factor_evaluate(variable_id, 0, factors(i)._2, factors(i)._5,AMap)
      new_value_1 = new_value_1 * factor_evaluate(variable_id, 1, factors(i)._2, factors(i)._5,AMap)
      i += 1
    }
    val denominator = new_value_0+new_value_1
    var prob:Double = 0
    if(denominator == 0) prob = 0 else prob = new_value_0/denominator
    var new_value:Double = 0
    if(Random.nextFloat()>prob) 1 else 0
  }
  def gibbs(vcc:Dataset[VQ], A:RDD[(Long,Double)], iterations:Int):DataFrame = {
    //var q = compute_q(vcc, A)
    var adf = A.toDF("variable_id","sw0")
    var AMap = A.collectAsMap
    val q_grouped = compute_QGrouped(vcc)
    for(i <- 1 to iterations) {
      println(AMap)
      val new_A: Map[Long, Double] = q_grouped.map{
        r => {
          val x = sample(r.variable_id, r.factors,AMap)
          (r.variable_id, x)
        }
      }.collect().toMap
      AMap = new_A
      val new_adf = spark.sparkContext.parallelize(AMap.toList).toDF("variable_id","new_value")
      adf = adf.as("A").join(new_adf.as("new_adf"), $"A.variable_id" === $"new_adf.variable_id").select("A.*","new_adf.new_value").withColumnRenamed("new_value", "sw"+i.toString)
    }
    adf.cache
  }
}
