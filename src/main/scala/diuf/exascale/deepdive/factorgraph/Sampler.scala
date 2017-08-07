package diuf.exascale.deepdive.factorgraph


/**
  * Created by sam on 8/3/17.
  */
import diuf.exascale.deepdive.factorgraph.Materialization.{compute_QGrouped, compute_q}
import org.apache.spark.sql.Dataset

import scala.util.Random
object Sampler {
  import Engine.spark.implicits._
  def random_assignment(variables: Dataset[Variable]): Dataset[VariableAssignment] = {
    variables.map {
      r => {
        if (!r.isEvidence) {
          val rnd = new scala.util.Random
          (r.id, rnd.nextInt(r.cardinality.toInt).toDouble)
        }
        else (r.id, r.variable_initial_value)
      }
    }.toDF("variable_id", "value").as[VariableAssignment] //maybe no need for this case class?
  }
  def factor_evaluate(variable_id:Long, variable_value:Double, func:Short, function_arguments:Array[(Long, Boolean)], values:Array[(Long,Boolean,Double)]):Double = {
    func match {
     case 0 => //implication
       var a = false
       var b = false
       if(function_arguments.length == 2) {
         if (function_arguments(0)._1 == variable_id) {
           a = if (variable_value == 1) true else false
           b = if (values.filter(_._1 == function_arguments(1)._1)(0)._3 == 1) true else false
         } else {
           b = if (variable_value == 1) true else false
           a = if (values.filter(_._1 == function_arguments(0)._1)(0)._3 == 1) true else false
         }
       }else if(function_arguments.length == 1){
         b = if (variable_value == 1) true else false
         a = if (values.filter(_._1 == function_arguments(0)._1)(0)._3 == 1) true else false
       }

       if (!a || b) 1 else 0
    }
  }
  def sample(variable_id:Long, factors:Array[(Long,Short,Double,Long,Array[(Long, Boolean)])], variables:Array[(Long,Boolean,Double)]):Double = {
    var i = 0
    var new_value_0:Double = 1
    var new_value_1:Double = 1
    while (i<factors.length){
      //for boolean
      new_value_0 = new_value_0 * factor_evaluate(variable_id, 0, factors(i)._2, factors(i)._5, variables)
      new_value_1 = new_value_1 * factor_evaluate(variable_id, 1, factors(i)._2, factors(i)._5, variables)
      i += 1
    }
    val denominator = new_value_0+new_value_1
    var prob:Double = 0
    if(denominator == 0) prob = 0 else prob = new_value_0/denominator
    println(variable_id,"for x = 0", new_value_0,"for x = 1",new_value_1, prob)
    if(Random.nextFloat()>prob) 1 else 0
  }
  def gibbs(vcc:Dataset[VQ],A:Dataset[VariableAssignment], iterations:Int):Unit = {
    var q = compute_q(vcc, A)
    var q_grouped = compute_QGrouped(q).cache
    q_grouped.show(20,false)
    for(i <- 1 to iterations) {
      val sw: Dataset[VariableAssignment] = q_grouped.map {
        r => VariableAssignment(r.variable_id, sample(r.variable_id, r.factors, r.variables))
      }.cache
      q = compute_q(vcc, sw)
      q_grouped = compute_QGrouped(q)
      sw.show
      sw.unpersist
    }
  }
}
