package diuf.exascale.deepdive.factorgraph


/**
  * Created by sam on 8/3/17.
  */
import org.apache.spark.sql.{Dataset}
object Sampler {
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
  def sample(factors:Array[(Long,Short,Double,Long,Array[(Long, Boolean)])], variables:Array[(Long,Boolean,Double)]):Double = {
    var new_value = 0.0
    new_value
  }
  def gibbs(Q:Dataset[QGroup], iterations:Long):Unit = {
    val sw:Dataset[VariableAssignment] = Q.map{
      r => VariableAssignment(r.variable_id, sample(r.factors, r.variables))
    }
  }
}
