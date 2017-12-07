package org.training.spark.DefectGraphOperation

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object mageGraph {

  /**
    * 将两个图合并成一个
    *
    * @param g1
    * @param g2
    * @return
    */
  def megerGraphs(g1: Graph[String, String], g2: Graph[String, String]): Graph[String, String] = {
    // 构建共用的顶点字典
    val v: RDD[(String, Long)] =
    // 1.通过g1.vertices.map(_._2)生成第一个输入图包含顶点属性值的RDD
      g1.vertices.map(_._2).
        // 2.通过g2.vertices.map(_._2)生成第二个输入图包含顶点属性值的RDD,并将两个RDD合并
        union(g2.vertices.map(_._2))
        // 3.对合并后的RDD进行去重操作，使用distinct生成一个顶点属性值不重复的RDD
        .distinct
        // 4.通过zipWithIndex为每个顶点生成新的顶点ID
        .zipWithIndex()
    Graph(v.map(_.swap), edgesWithNewVertexIds(g1, v).union(edgesWithNewVertexIds(g2, v)))
  }

  /**
    * 将图的边集和V内的VertexIds映射起来，在输入G1和G2执行相同的操作，
    * 最终的返回结果是一个对图G1和G2的边都进行了映射的新图(新图当然也会包含v中的顶点)
    *
    * @param g
    * @param v
    * @return
    */
  def edgesWithNewVertexIds(g: Graph[String, String], v: RDD[(String, Long)]): RDD[Edge[String]] = {
    g.triplets
      .map(et => (et.srcAttr, (et.attr, et.dstAttr)))
      .join(v)
      .map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
      .join(v)
      .map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2))
  }

  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")
    val phiosophers: Graph[String, String] = Graph(
      sc.makeRDD(Seq(
        (1l, "Aristotle"), (2l, "Plato"), (3l, "Socrates"), (4l, "male")
      )),
      sc.makeRDD(Seq(
        Edge(2l, 1l, "Influences"),
        Edge(3l, 2l, "Influences"),
        Edge(3l, 4l, "hasGender")
      ))
    )
    val rdfGraph: Graph[String, String] = Graph(
      sc.makeRDD(Seq(
        (1l, "wordnet_philosophers"), (2l, "Aristotle"), (3l, "Plato"), (4l, "Socrates")
      )),
      sc.makeRDD(Seq(
        Edge(2l, 1l, "rdf:type"),
        Edge(3l, 1l, "rdf:type"),
        Edge(4l, 1l, "rdf:type")
      ))
    )

    val combined = megerGraphs(phiosophers, rdfGraph)
    combined.triplets.foreach(t=>println(s"${t.srcAttr} --- ${t.dstAttr} ---> ${t.attr}"))

  }
}
