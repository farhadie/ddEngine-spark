package diuf.exascale.deepdive.factorgraph

/**
  *
  * get the csv files for factors and variables and converts them to a rdd of Variable or Factor class
  */

import diuf.exascale.deepdive.factorgraph.Engine.spark
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{collect_set, monotonically_increasing_id, struct}

object Materialization {
  import Engine.spark.implicits._
  def clean_weights(base_dir: String): Dataset[Weight] = {
    val weightdf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
      .load("/" + base_dir + "/weights")
    weightdf.map {
      r => Weight(r.getInt(0), r.getBoolean(1), r.getDouble(2))
    }
  }
  def clean_variables(base_dir: String): Dataset[Variable] = {
    val vardf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
      .load("/" + base_dir + "/variables")
    vardf.map {
      r => Variable(r.getInt(0), r.getBoolean(1), r.getDouble(2), r.getInt(3).toShort, r.getInt(5))
    }
  }
  def clean_factors(base_dir: String, weights: Dataset[Weight]): Dataset[Factor] = {
    val fdf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true").load("/" + base_dir + "/factors").withColumn("id", monotonically_increasing_id() + 10)
    fdf.as("factors").join(weights.as("weights"), $"factors._c0" === $"weights.id").map{ //joins factor with weights
      r => {
        val s = r.size - 1
        var array: Array[(Long, Boolean)] = Array()
        val str = r.getString(4).drop(1).dropRight(1).split(", ").toList
        for (i <- str.indices by 2) {
          array = array :+ (str(i).toLong, str(i + 1).toBoolean)
        }
        Factor(r.getLong(s-3), r.getInt(0), r.getInt(1).toShort, r.getInt(3), array, r.getDouble(s))
      }
    }
  }
  def clean_edges(factors: Dataset[Factor], variables: Dataset[Variable]): Dataset[Edge] = {
    val raw_edges: Dataset[(Long, Long, Short,Double,Long, Array[(Long, Boolean)])] = factors.flatMap {
      f => {
        f.variables.map {
          v => (v._1, f.id, f.func, f.weight,f.weight_id, f.variables)
        }
      }
    }
      raw_edges.join(variables, raw_edges("_1") === variables("id")).select(variables("id").cast("Long").as("variable_id"),
        variables("isEvidence").as("isEvidence"),variables("variable_initial_value").as("variable_initial_value"),
        raw_edges("_2").as("factor_id"), raw_edges("_3").as("func"), raw_edges("_4").as("weight"),
        raw_edges("_5").as("weight_id"), raw_edges("_6").as("factor_variables")).as[Edge]
  }
  // variable-side co-clustering
  def vcc(E: Dataset[Edge]):Dataset[VQ] = {
    E.as("E1").join(E.as("E2"), $"E1.factor_id" === $"E2.factor_id")//.where($"E1.variable_id" =!= $"E2.variable_id")
      .select("E1.*","E2.variable_id","E2.isEvidence","E2.variable_initial_value")
      .toDF("variable_id", "isEvidence", "variable_initial_value", "factor_id", "func", "weight", "weight_id", "factor_variables", "variable2_id",
        "variable2_isEvidence", "variable2_initial_value").as[VQ].cache() //TODO performance
  }
  // factor-side co-clustering
  def fcc(E: Dataset[Edge], A:Dataset[VariableAssignment]):Dataset[FQ] = {
    E.as("E").join(A.as("A")).where($"E.variable_id" === $"A.variable_id").select("E.*", "A.value")
      .toDF("variable_id", "isEvidence", "variable_initial_value", "factor_id", "func", "weight", "weight_id", "factor_variables", "value")
      .as[FQ].cache

  }
  // Q

  def compute_QGrouped(q:Dataset[VQ]):Dataset[QGroup]= {
    q.groupBy("variable_id").agg(collect_set(struct("factor_id","func","weight","weight_id","factor_variables")).as("factors")).as[QGroup]
  }
//  def compute_q(vcc:Dataset[VQ],A:Dataset[VariableAssignment]):Dataset[Q] = {
//    vcc.as("VQ").join(A.as("A"), $"VQ.variable2_id" === $"A.variable_id").select("VQ.*", "A.value").
//      toDF("variable_id", "isEvidence", "variable_initial_value", "factor_id", "func", "weight", "weight_id", "factor_variables", "variable2_id",
//        "variable2_isEvidence", "variable2_initial_value", "value").as[Q].cache
//  }
//  def compute_QGrouped(q:Dataset[Q]):Dataset[QGroup]= {
//    q.groupBy("variable_id").agg(collect_set(struct("factor_id","func","weight","weight_id","factor_variables")).as("factors")
//      ,collect_set(struct("variable2_id", "variable2_isEvidence", "value")).as("variables")).as[QGroup]
//  }
}
