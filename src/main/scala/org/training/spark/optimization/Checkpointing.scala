package org.training.spark.optimization

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.storage.StorageLevel
import org.training.spark.main.Singleton

object Checkpointing {
  val sc = Singleton.getSingleton
  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    // 设置checkpoint目录，如果不设置,随后的checkpoint不会有任何效果
    val path = "hdfs://host:8020/tmp/spark-checkpoint"
    sc.setCheckpointDir(path)
    // 记录更新了多少次图
    var updateCount: Int = 0
    // 每50次图更新做一次checkpoint
    val checkpointInterval: Int = 50

    val iterations = 500
    var g: Graph[Int, Int] = Graph.fromEdges(sc.makeRDD(Seq(Edge(1l, 3l, 1),
      Edge(2l, 4l, 1), Edge(3l, 4l, 1))), 1)
    update(g)
    g.vertices.count
    // 在每次更新图后调用update方法，然后执行具体的算法实现，否则就没有更新和checkpoint的内容
    for (i <- 1 to iterations) {
      println("Iterations: " + i)
      val newGraph =
        g.mapVertices((_, vd) => (vd * i) / 17)
      g = g.outerJoinVertices[Int, Int](newGraph.vertices) {
        (_, _, newData) => newData.getOrElse(0)
      }
      update(g)
      g.vertices.count
    }
    g.vertices.collect.foreach(println)


    /**
      * update函数在每次更新图的时候调用,其中缓存和checkpoint是必需的
      *
      * @param newData
      */
    def update(newData: Graph[Int, Int]): Unit = {
      newData.persist(StorageLevel.MEMORY_ONLY)
      updateCount += 1
      if (updateCount % checkpointInterval == 0) {
        newData.checkpoint
      }
    }

  }


}
