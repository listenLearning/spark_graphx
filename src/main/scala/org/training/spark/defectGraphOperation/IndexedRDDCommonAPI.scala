package org.training.spark.defectGraphOperation

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.training.spark.main.Singleton

object IndexedRDDCommonAPI {
  def main(args: Array[String]): Unit = {
    val sc = Singleton.getSingleton
    sc.setLogLevel("WARN")

    // Create an RDD of key-value pairs with Long keys.
    val rdd = sc.parallelize((1 to 1000000).map(x => (x.toLong, 0)))
    // Construct an IndexedRDD from the pairs, hash-partitioning and indexing
    // the entries.
    val indexed = IndexedRDD(rdd).cache()

    // Perform a point update.
    val indexed2 = indexed.put(1234L, 10873).cache()
    // Perform a point lookup. Note that the original IndexedRDD remains
    // unmodified.
    indexed2.get(1234L) // => Some(10873)
    indexed.get(1234L) // => Some(0)

    // Efficiently join derived IndexedRDD with original.
    val indexed3 = indexed.innerJoin(indexed2) { (id, a, b) => b }.filter(_._2 != 0)
    indexed3.collect // => Array((1234L, 10873))

    // Perform insertions and deletions.
    val indexed4 = indexed2.put(-100L, 111).delete(Array(998L, 999L)).cache()
    indexed2.get(-100L) // => None
    indexed4.get(-100L) // => Some(111)
    indexed2.get(999L) // => Some(0)
    indexed4.get(999L) // => None

  }
}
