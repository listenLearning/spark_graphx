package org.training.spark.ClassicalAlgorithm

import java.io.PrintWriter

import org.apache.spark.graphx.{Edge, Graph}
import org.training.spark.CommonAPI.Gexf
import org.training.spark.main.Singleton

/**
  * 定义：
  * 一个完整的图是每一个顶点都有一个和其他定点连接的边。图中的边数e和定点数v之间的函数关系是：e=n(n-1)/2
  * 说明：
  * 每一个动物都是一个顶点，动物之间的连接是有权值的边，边的权值来自于动物向量中的余弦距离，
  * 由于为每两个动物都生成一条边，所以图是完整的
  * 这个案例展示了如何获得准确的一组关系之间的语义连接，此时使用的是动物名称的例子，
  * 类似的方法也可以用于其他有大量非结构化文本数据的领域，如：医学文献或全球证券交易所上市公司的报告
  */
object Demo12_Animal_Distances {

  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")
    val dist = sc.textFile("Data/animal_distances.txt")

    val verts =
    // animal_distances.txt文件中的前两列包含动物名，选择第一列，应用距离函数来保证顶点唯一
      dist.map(_.split(",")(0)).distinct()
        //调用hashcode，并转成Long值，这个Long值作为每个顶点个的VertexId
        .map(x => (x.hashCode.toLong, x))
    val edges = dist.map(_.split(",")).map(x =>
      Edge(x(0).hashCode.toLong, x(1).hashCode.toLong, x(2).toDouble))
    val distg = Graph(verts, edges = edges)
    // 调用之前Demo11_Krushal写过的最小生成树算法
    val mst = Demo11_Krushal.minSpanningTree(distg)
    val pw = new PrintWriter("animal_distances.gexf")
    pw.write(Gexf.toGexf(mst))
    pw.close()
  }
}
