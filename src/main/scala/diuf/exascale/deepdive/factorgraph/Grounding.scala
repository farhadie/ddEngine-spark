package diuf.exascale.deepdive.factorgraph

/**
  *
  * get the csv files for factors and variables and converts them to a rdd of Variable or Factor class
  */
import diuf.exascale.deepdive.factorgraph.Engine.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.monotonically_increasing_id

object Grounding {
  import Engine.spark.implicits._
  def clean_weights(base_dir:String): RDD[Weight] = {
    val weightdf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
      .load("/" + base_dir + "/weights")
    weightdf.map{
      r => Weight(r.getInt(0), r.getDouble(1), r.getBoolean(2))
    }.rdd.cache
  }
  def clean_variables(base_dir:String): RDD[(Variable)] = {
    val vardf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
      .load("/" + base_dir + "/variables")
    vardf.map{
      r => Variable(r.getInt(0), r.getBoolean(1), r.getDouble(2), r.getInt(3).toShort,r.getInt(5))
    }.rdd.cache
  }
  def clean_factors(base_dir:String): RDD[Factor] = {
    val fdf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
      .load("/" + base_dir + "/factors").withColumn("id", monotonically_increasing_id()+10)
    fdf.map{
      r => {
        val s = r.size - 1
        var array: Array[(Long, Boolean)] = Array()
        val str = r.getString(4).drop(1).dropRight(1).split(", ").toList
        for(i <- str.indices by 2){
          array = array :+ (str(i).toLong, str(i+1).toBoolean)
        }
        Factor(r.getLong(s),r.getInt(0), r.getInt(1).toShort, r.getInt(3), array, 0.0)
      }
    }.rdd.cache
  }
}
