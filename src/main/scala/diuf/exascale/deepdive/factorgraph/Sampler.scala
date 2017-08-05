package diuf.exascale.deepdive.factorgraph


/**
  * Created by sam on 8/3/17.
  */
import org.apache.spark.sql.Dataset
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
  def factor_evaluate(variable_id:Long, func:Short, values:Array[(Long,Boolean,Double)]):Double = {
    func match {
     case 0 => //implication
       var a:Boolean = false
       var b:Boolean = false
       if(values(0)._1 == variable_id){
         b = if(values(0)._3 == 1) true else false
         a = if(values(1)._3  == 1) true else false
       }else{
         a = if(values(0)._3 == 1) true else false
         b = if(values(1)._3  == 1) true else false
       }
       if (!a || b) 1 else 0
    }
  }
  def sample(variable_id:Long, factors:Array[(Long,Short,Double,Long,Array[(Long, Boolean)])], variables:Array[(Long,Boolean,Double)]):Double = {
    var i = 0
    var new_value:Double = 0
    while (i<factors.length){
      new_value = new_value + (factor_evaluate(variable_id, factors(i)._2, variables)*factors(i)._3)
      i += 1
    }
    new_value
  }
  def gibbs(Q:Dataset[QGroup], iterations:Int):Unit = {
    for(i <- 1 to iterations) {
      val sw: Dataset[VariableAssignment] = Q.map {
        r => VariableAssignment(r.variable_id, sample(r.variable_id, r.factors, r.variables))
      }
      sw.show
    }
  }
}
