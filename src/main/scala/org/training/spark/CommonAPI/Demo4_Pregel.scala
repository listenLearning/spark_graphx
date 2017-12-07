package org.training.spark.CommonAPI

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Pregel, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.training.spark.main.Singleton

object Demo4_Pregel {
  val sc = Singleton.getSingleton
  sc.setLogLevel("DEBUG")

  def main(args: Array[String]): Unit = {
    val graph = GraphGenerators.rmatGraph(sc, 16, 32)
    val g = Pregel(
      graph.mapVertices((vid, vd) => 0), 0, activeDirection = EdgeDirection.Out
    )(
      (id: VertexId, vd: Int, a: Int) => math.max(vd, a),
      (et: EdgeTriplet[Int, Int]) => Iterator((et.dstId, et.srcAttr + 1)),
      (a: Int, b: Int) => math.max(a, b)
    )

    g.vertices.collect.foreach(println)
  }
}
