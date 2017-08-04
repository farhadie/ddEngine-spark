package diuf.exascale.deepdive.factorgraph

/**
  *
  * get the csv files for factors and variables and converts them to a rdd of Variable or Factor class
  */

import diuf.exascale.deepdive.factorgraph.Engine.spark
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.monotonically_increasing_id

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
  def clean_factors(base_dir: String): Dataset[Factor] = {
    val fdf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
      .load("/" + base_dir + "/factors").withColumn("id", monotonically_increasing_id() + 10)
    fdf.map {
      r => {
        val s = r.size - 1
        var array: Array[(Long, Boolean)] = Array()
        val str = r.getString(4).drop(1).dropRight(1).split(", ").toList
        for (i <- str.indices by 2) {
          array = array :+ (str(i).toLong, str(i + 1).toBoolean)
        }
        Factor(r.getLong(s), r.getInt(0), r.getInt(1).toShort, r.getInt(3), array, 0.0)
      }
    }
  }
  def clean_edges(factors: Dataset[Factor], variables: Dataset[Variable]): Dataset[Edge] = {
    val raw_edges: Dataset[(Long, Long, Short,Double,Long)] = factors.flatMap {
      f => {
        f.variables.map {
          v => (v._1, f.id, f.func, f.weight,f.weight_id)
        }
      }
    }
      raw_edges.join(variables, raw_edges("_1") === variables("id")).select(variables("id").cast("Long").as("variable_id"),
        variables("isEvidence").as("isEvidence"),variables("variable_initial_value").as("variable_initial_value"),
        raw_edges("_2").as("factor_id"), raw_edges("_3").as("func"), raw_edges("_4").as("weight"),
        raw_edges("_5").as("weight_id")).as[Edge]
  }
  // variable-side co-clustering
  def vcc(E: Dataset[Edge]):Dataset[VQ] = {
    E.as("E1").join(E.as("E2"), $"E1.factor_id" === $"E2.factor_id").where($"E1.variable_id" =!= $"E2.variable_id")
      .select("E1.*","E2.variable_id","E2.isEvidence","E2.variable_initial_value")
      .toDF("variable_id", "isEvidence", "variable_initial_value", "factor_id", "func", "weight", "weight_id", "variable2_id",
        "variable2_isEvidence", "variable2_initial_value").as[VQ].cache() //TODO performance
  }
  // factor-side co-clustering
  def fcc(E: Dataset[Edge], A:Dataset[VariableAssignment]):Dataset[FQ] = {
    E.as("E").join(A.as("A")).where($"E.variable_id" === $"A.variable_id").select("E.*", "A.value")
      .toDF("variable_id", "isEvidence", "variable_initial_value", "factor_id", "func", "weight", "weight_id", "value")
      .as[FQ].cache

  }
  // Q
  def compute_q(vcc:Dataset[VQ],A:Dataset[VariableAssignment]):Dataset[Q] = {
    vcc.as("VQ").join(A.as("A"), $"VQ.variable2_id" === $"A.variable_id").select("VQ.*", "A.value").
      toDF("variable_id", "isEvidence", "variable_initial_value", "factor_id", "func", "weight", "weight_id", "variable2_id",
        "variable2_isEvidence", "variable2_initial_value", "value").as[Q].cache
  }
}
