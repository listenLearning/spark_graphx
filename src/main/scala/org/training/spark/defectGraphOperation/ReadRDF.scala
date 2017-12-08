package org.training.spark.defectGraphOperation

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.training.spark.main.Singleton

object ReadRDF {
  /**
    * 读取Tab分割的RDF文件
    *
    * @param sc
    * @param fileName
    * @return
    */
  def readRdf(sc: SparkContext, fileName: String): Graph[String, String] = {
    val r: RDD[Array[String]] = sc.textFile(fileName).map(_.split("\t"))
    // 使用类似megerGraph函数类似的方法,来创建一个包含不重复顶点的字典
    val v: RDD[(String, Long)] = r.map(_ (1)).union(r.map(_ (3))).distinct.zipWithIndex
    Graph(v.map(_.swap), // 通过交换元素操作,将存储顶点名称与ID关系的RDD转换成VertexRDD
      // 首先将主语定点(即起始点)作为顶点的ID,然后再对宾语顶点(即目标顶点)执行类似的操作
      // 最后使用这两个顶点ID和原始的边属性来构建新的边Edge
      r.map(x => (x(1), (x(2), x(3)))) // (SubjectName,(Predicate,ObjectName))
        .join(v) // (SubjectName,((Predicate,ObjectName),SubjectId))
        .map(x => (x._2._1._2, (x._2._2, x._2._1._1))) // (ObjectName,(SubjectId,Predicate))
        .join(v) // (ObjectName,((SubjectId,Predicate),ObjectId))
        .map(x => Edge(x._2._1._1, x._2._2, x._2._1._2))) // Edge(SubjectId,Predicate,ObjectId)
  }

  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")
    val t0 = System.currentTimeMillis()
    val rd: Graph[String, String] = readRdfIndexed(sc, "yago/yagoSimpleTaxonomy.tsv")
    println("#edges = " + rd.edges.count + " #Vertices = " + rd.vertices.count)
    val t1 = System.currentTimeMillis()
    println("Elapsed: " + ((t1 - t0) / 1000) + "sec")
    sc.stop()
  }

  /**
    * IndexedRDD使用还有待商榷
    * 有兴趣的可以去github上看看
    * https://github.com/amplab/spark-indexedrdd
    *
    * @param sc
    * @param filename
    * @return
    */
  def readRdfIndexed(sc: SparkContext, filename: String) = {
    val r: RDD[Array[String]] = sc.textFile(filename).map(_.split("\t"))
    val v: IndexedRDD[String, Long] = IndexedRDD(r.map(_ (1)).union(r.map(_ (3))).distinct
      .zipWithIndex)

    val ir: RDD[(String, (String, String))] = r.map(x => (x(1), (x(2), x(3))))

    val rd: IndexedRDD[String, ((String, String), Long)] = IndexedRDD(ir).innerJoin(v)((_, a, b) => (a, b))
    val result: RDD[(String, (Long, String))] = rd.map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
    val re: IndexedRDD[String, (Long, String)] = IndexedRDD(result)
    val re1: IndexedRDD[String, ((Long, String), Long)] = re.innerJoin(v)((_, a, b) => (a, b))
    val re2: RDD[Edge[String]] = re1.map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2))

    Graph(v.map(_.swap), re2)
  }

}
