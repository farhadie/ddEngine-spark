package diuf.exascale.deepdive.factorgraph



/**
  * Created by Ehsan.
  */
case class Weight(id: Long, isFixed:Boolean, weight:Double) extends Serializable
case class Variable(id: Long, isEvidence: Boolean, var variable_initial_value: Double, data_type: Short, cardinality: Long) extends Serializable {
  def get_value(): Double = {
    variable_initial_value
  }
  def canEqual(a: Any): Boolean = a.isInstanceOf[Variable]

  override def equals(that: Any): Boolean = {
    that match {
      case that: Variable => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  }
  override def hashCode: Int = {
    val prime: Double = 31
    var result: Double = 1
    result = prime * result + variable_initial_value
    result = prime * result + isEvidence.hashCode
    result.toInt
  }
}
case class Factor(id: Long, var weight_id: Long, func: Short, edge_count: Long, variables: Array[(Long, Boolean)], var weight: Double) extends Serializable {
  def call(methodName: String, args: AnyRef*): AnyRef = {
    def argtypes = args.map(_.getClass)

    def method = Factor.getClass.getMethod(methodName, argtypes: _*)

    method.invoke(Factor, args: _*)
  }

  def is_connected(variable_id: Long): Boolean = {
    variables.map(c => c._1).contains(variable_id)
  }

  def map_func(): String = {
    "implication"
  }

  def set_weight(new_weight:Double): Unit = {
    weight = new_weight
  }

  def toBool(a: Long): Boolean = if (a == 1) true else false

  def toInt(a: Boolean): Double = if (a) 1 else 0

  def evaluate(): Double = {
    toInt(implies(toBool(variables(0)._1), toBool(variables(1)._1)))
  }

  def implies(a: Boolean, b: Boolean): Boolean = !a || b

  def negation(a: Boolean): Boolean = !a

  def equality(a: Boolean, b: Boolean): Boolean = a == b

  def disjunction(a: Boolean, b: Boolean): Boolean = a && b

  def conjunction(a: Boolean, b: Boolean): Boolean = a || b

  override def equals(that: Any): Boolean = that match {
    case that: Factor => that.canEqual(this) && this.hashCode == that.hashCode
    case _ => false
  }

  override def hashCode: Int = {
    val prime: Double = 31
    var result: Double = 1
    result = prime * result + weight_id
    result = result.hashCode()
    result.toInt
  }
}
case class Edge(variable_id: Long, isEvidence:Boolean, variable_initial_value:Double, factor_id: Long, func:Short, weight:Double, weight_id:Long, factor_variables: Array[(Long, Boolean)]) extends Serializable {}
case class VariableAssignment(value:Double, variable_id:Long)
case class VQ(variable_id: Long, isEvidence:Boolean, variable_initial_value:Double, factor_id: Long, func:Short, weight:Double, weight_id:Long, factor_variables: Array[(Long, Boolean)], variable2_id: Long, variable2_isEvidence:Boolean, variable2_initial_value:Double)
case class FQ(variable_id: Long, isEvidence:Boolean, variable_initial_value:Double, factor_id: Long, func:Short, weight:Double, weight_id:Long, factor_variables: Array[(Long, Boolean)], value:Double)
//case class Q(variable_id: Long, isEvidence:Boolean, variable_initial_value:Double, factor_id: Long, func:Short, weight:Double, weight_id:Long, factor_variables: Array[(Long, Boolean)], variable2_id: Long, variable2_isEvidence:Boolean, variable2_initial_value:Double, value:Double)
//case class QGroup(variable_id: Long, factors:Array[(Long,Short,Double,Long,Array[(Long, Boolean)])], variables:Array[(Long,Boolean,Double)])
case class Q(variable_id: Long, isEvidence:Boolean, variable_initial_value:Double, factor_id: Long, func:Short, weight:Double, weight_id:Long, factor_variables: Array[(Long, Boolean)], variable2_id: Long, variable2_isEvidence:Boolean, variable2_initial_value:Double)
case class QGroup(variable_id: Long, factors:Array[(Long,Short,Double,Long,Array[(Long, Boolean)])])
