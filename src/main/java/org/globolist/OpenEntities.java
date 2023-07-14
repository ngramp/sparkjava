package org.globolist;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.globolist.utils.MySpark;
import org.globolist.utils.OpenNlp;
import org.globolist.utils.Text;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class OpenEntities {
    public static void main(String[] args) {
        final String nerfile = args[0];

        // Create a JavaSparkContext
        try(JavaSparkContext sc = MySpark.createJSC()) {

            JavaPairRDD<String, PortableDataStream> warcFilesRDD = sc.binaryFiles("/home/gram/Downloads/*.warc.gz");

            // Extract named entities from each text file
            JavaRDD<String> namedEntitiesRDD = warcFilesRDD.flatMap(file -> {
                List<String> namedEntities = new ArrayList<>();
                WarcReader reader = new WarcReader(file._2().open());
                for(WarcRecord record : reader){
                    Map<String, List<String>> headers = record.headers().map();
                    if(headers.containsKey("WARC-Target-URI")
                            && headers.containsKey("WARC-Type")){
                        if(!headers.get("WARC-Target-URI").isEmpty()
                                && headers.get("WARC-Type").contains("response")){
                            //do something
                            String text = Text.getRecordBody(record);
                            namedEntities.addAll(OpenNlp.findNamedEntities(nerfile,text));
                        }
                    }
                }

                return namedEntities.iterator();
            });

            // Count the occurrences of each named entity
            Map<String, Long> entityCounts = namedEntitiesRDD.countByValue();

            // Sort the named entities by count in descending order
            List<Map.Entry<String, Long>> sortedEntities = new ArrayList<>(entityCounts.entrySet());
            sortedEntities.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

            // Get the top 50 most common named entities
            List<Map.Entry<String, Long>> topEntities = sortedEntities.subList(0, Math.min(sortedEntities.size(), 50));

            // Print the top 50 named entities
            for (Map.Entry<String, Long> entry : topEntities) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            // Stop the SparkContext
            sc.stop();
        }

    }
}