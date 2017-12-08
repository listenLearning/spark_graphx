package org.training.spark.DefectGraphOperation

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

import scala.reflect.ClassTag

object ClusteringCoefficient {

  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  /**
    * 全局聚类系数
    *
    * @param g
    * @tparam VD
    * @tparam ED
    * @return
    */
  def clusteringCoefficient[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]): Double = {
    val numTriplets =
      g.aggregateMessages[Set[VertexId]](
        et => {
          et.sendToSrc(Set(et.dstId))
          et.sendToDst(Set(et.srcId))
        },
        (a, b) => a ++ b
      ).map(x => {
        val s = (x._2 - x._1).size;
        s * (s - 1) / 2
      }).reduce(_ + _)
    if (numTriplets == 0)
      0.0
    else
      g.triangleCount.vertices.map(_._2).reduce(_ + _) / numTriplets.toDouble
  }

  def main(args: Array[String]): Unit = {
    val path = "facebook/0.edges"
    val g: Graph[Int, Int] = GraphLoader.edgeListFile(sc, path)
    val feat: RDD[(VertexId, Boolean)] = sc.textFile("facebook/0.feat")
      .map(x => {
        val array = x.split(" ")
        (array(0).toLong, array(1).toInt == 1)
      })
    val g2: Graph[Boolean, Int] = g.outerJoinVertices(feat)((_, _, u) => u.get)

    clusteringCoefficient(g2)
    clusteringCoefficient(g2.subgraph(_ => true, (_, vd) => vd))
    clusteringCoefficient(g2.subgraph(_ => true, (_, vd) => !vd))
  }
}
