package diuf.exascale.deepdive.factorgraph

/**
  * Created by Ehsan.
  */
import org.apache.spark.rdd.RDD

object Inference {
  def crf(): Unit = {}

  def sample(graph: Graph, mb: RDD[(Long, Iterable[Long])], vertex_id: Long): Variable = {
    Variable(0,false,0.0,0,2)
  }

  def random_assignment(variables: RDD[Variable]): RDD[Variable] = {
    val state: RDD[Variable] = variables.map {
      r => {
        if (!r.isEvidence) {
          val rnd = new scala.util.Random
          Variable(r.id,r.isEvidence, rnd.nextInt(r.cardinality.toInt).toDouble, r.data_type, r.cardinality)
        }
        else r
      }
    }
    state
  }

  def gibbs_sampler(graph: Graph, steps: Int, burn_out: Long, thin: Long): Unit = {
    val mb = graph.markov_blanket()
    var Xt: RDD[Variable] = random_assignment(graph.variables)
    var states: Array[Array[Variable]]= Array(Xt.collect)
    states.foreach{x=> x.foreach(println);println("___");}

    for (i <- 1 to steps) {
      Xt.foreach(println)
      println("in loop----")
      //TODO maybe change it with foreach for better convergence
      val Xt1: RDD[Variable] = Xt.map{
        r => sample(Graph(Xt, graph.factors, graph.weight), mb, r.id)
      }
      if(i>burn_out && i%thin==0){
      states = states :+ Xt1.collect
      }
      Xt = Xt1
    }
    println(states.length)
  }
}
