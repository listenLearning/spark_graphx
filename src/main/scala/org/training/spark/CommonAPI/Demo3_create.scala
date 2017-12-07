package org.training.spark.CommonAPI

import java.io.PrintWriter

import org.apache.spark.graphx.util.GraphGenerators
import org.training.spark.main.Singleton

object Demo3_create {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    // 网格图(网格图不存在随机性,并且网格是完整的)
    /*val graph = GraphGenerators.gridGraph(sc, 4, 4)
    graph.vertices.sortByKey().collect().foreach(println)
    println
    graph.edges.collect().foreach(println)
    println
    graph.triplets.collect().foreach(println)*/
    // 星形图(不是随机图)

    /*val graph = GraphGenerators.starGraph(sc,8)
    graph.vertices.sortByKey().collect().foreach(println)
    println
    graph.edges.collect().foreach(println)*/
    //随机图
    // 1.对数正态图(基于度的)
    // logNormalGraph函数的第二个参数指定了图的顶点总数
    /*val graph = GraphGenerators.logNormalGraph(sc,8)
    graph.vertices.sortByKey().collect().foreach(println)
    println
    graph.edges.collect().foreach(println)*/
    // 2.R-MAT图(基于程序化流程的,与现实世界相近)
    // rmatGraph函数的第二个和第三个参数是要求的定点数和独立的边数——定点数被取值为最近的一个2的幂值
    /*val graph = GraphGenerators.rmatGraph(sc,128,200)
    graph.vertices.sortByKey().collect().foreach(println)
    println
    graph.edges.collect().foreach(println)*/

    val pw = new PrintWriter("rmatGraph.gexf")
    pw.write(Gexf.toGexf(GraphGenerators.rmatGraph(sc, 32, 60)))
    pw.close()
  }
}
