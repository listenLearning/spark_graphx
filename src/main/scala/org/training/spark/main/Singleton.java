package org.training.spark.main;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class Singleton {
    private volatile static SparkContext sparkContext;
    private volatile static SparkConf conf ;
    private Singleton() {
    }

    public static SparkContext getSingleton() {
        if (sparkContext == null) {
            synchronized (Singleton.class) {
                conf = new SparkConf().setAppName("spark_graph_demo").setMaster("local");
                sparkContext = SparkContext.getOrCreate(conf);
            }
        }
        return sparkContext;
    }
}
