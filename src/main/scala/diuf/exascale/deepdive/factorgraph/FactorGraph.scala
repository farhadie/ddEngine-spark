package diuf.exascale.deepdive.factorgraph

import org.apache.spark.rdd.RDD

/**
  * Created by Ehsan.
  */
class VertexProperty() extends Serializable
case class Weight(id: Long, weight:Double, isFixed:Boolean) extends Serializable
case class Variable(id: Long, isEvidence: Boolean, var value: Double, data_type: Short, cardinality: Long) extends VertexProperty {
  def get_value(): Double = {
    value
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
    result = prime * result + value
    result = prime * result + isEvidence.hashCode
    result.toInt
  }
}
case class Factor(id: Long, var weight_id: Long, func: Short, edge_count: Long, variables: Array[(Long, Boolean)], var weight: Double) extends VertexProperty {
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
case class MarkovBlanket(id:Long, factors: Array[Factor], variables: Array[Variable])
case class Graph(variables: RDD[Variable], factors: RDD[Factor], weight: RDD[Weight]){
  def markov_blanket(): RDD[(Long, Iterable[Long])] = {
    val edges = factors.flatMap{
      r => r.variables.map(v => (r.id, v._1))
    }.groupBy(_._2).map{
      r => (r._1, r._2.map(_._1))
    }

    // variables connected to each other
    //    val super_edges = factors.flatMap{
    //      r => r.variables.toSet.subsets(2).toList
    //    }.flatMap{
    //      r => {
    //        val row = r.toArray
    //        Array((row(0)._1, row(1)._1), (row(1)._1, row(0)._1))
    //      }
    //    }.distinct.groupBy(_._1)
    edges
  }
}
