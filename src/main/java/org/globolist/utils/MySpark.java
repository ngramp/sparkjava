package org.globolist.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

public class MySpark implements Serializable {
    public static JavaSparkContext createJSC() {
        SparkConf conf = new SparkConf()
                .setAppName("My application")
                .setMaster("local[*]");
        return new JavaSparkContext(conf);
    }
    public static JavaStreamingContext createJSSC() {
        SparkConf conf = new SparkConf()
                .setAppName("My streaming application")
                .setMaster("local[*]");
        return new JavaStreamingContext(conf, new Duration(1000));
    }
}
