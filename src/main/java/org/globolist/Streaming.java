package org.globolist;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class Streaming{
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkSession spark = SparkSession.builder().appName("Streaming Application").getOrCreate();
        StructType schema = new StructType().add("line", DataTypes.StringType);

        Dataset<String> files = spark
                .readStream()
                .textFile("/home/gram/Downloads/*.warc.gz");

        Dataset<Row> lineCount = files.groupBy().count();

        StreamingQuery query = lineCount.writeStream()
          .outputMode("complete")
          .format("console")
          .start();

        query.awaitTermination();

    }
}
