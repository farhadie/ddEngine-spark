package diuf.exascale.deepdive.factorgraph

/**
  * Created by Ehsan.
  */
import org.apache.spark.sql.{DataFrame, Dataset}

object Inference {
  def infer(sample_worlds:DataFrame, factor: Dataset[Factor], iterations:Int, burnout:Int, thin:Int): Unit = {
    for(i <- 0 until iterations){
      val col = "sw" + i
    }
  }
  def pr_I(factor: Dataset[Factor], value:DataFrame):Double = {
    0.0
  }
}
