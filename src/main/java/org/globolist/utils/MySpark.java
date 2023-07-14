package org.globolist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class MySpark implements Serializable {
    public static JavaSparkContext createJSC() {
        SparkConf conf = new SparkConf().setAppName("My application")
                .setMaster("local[*]");
        return new JavaSparkContext(conf);
    }
}
