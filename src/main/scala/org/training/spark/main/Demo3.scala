package org.training.spark.main

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators

object Demo3 {
  val sc: SparkContext = Singleton.getSingleton

  def main(args: Array[String]): Unit = {
    val graph: Graph[Double, Int] = GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)

    //graph.triplets.take(10).foreach(println)

    /** Old func **/
    /*val olderFollowers = graph.mapReduceTriplets[(Int, Double)](
      triplet => {
        if (triplet.srcAttr > triplet.dstAttr) {
          Iterator((triplet.dstId, (1, triplet.srcAttr)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )

    val avgAgeOfOlderFollowers = olderFollowers.mapValues((id, value) => value match {
      case (count, totalAge) => totalAge / count
    })

    olderFollowers.take(10).foreach(println)*/

    /** new func **/
    val olderFollowers = graph.aggregateMessages[(Int, Double)](
      triplet => {
        if (triplet.srcAttr > triplet.dstAttr) {
          triplet.sendToDst(1, triplet.srcAttr)
        } else {
          triplet.sendToSrc(1, triplet.dstAttr)
        }
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )

    olderFollowers.take(10).foreach(println)


  }
}
