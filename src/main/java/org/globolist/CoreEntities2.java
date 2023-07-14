package org.globolist;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.globolist.utils.CoreNlp;
import org.globolist.utils.MySpark;
import org.globolist.utils.Text;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CoreEntities {
    public static void main(String[] args) {

        // Create a JavaSparkContext
        try(JavaSparkContext sc = MySpark.createJSC()) {
           // CoreNlp.loadModels(sc);

            JavaPairRDD<String, PortableDataStream> warcFilesRDD = sc.binaryFiles("/home/gram/Downloads/*.warc.gz");

            JavaRDD<String> bodies = warcFilesRDD.flatMap(file -> {
                WarcReader reader = new WarcReader(file._2().open());
                List<String> bodyList = new ArrayList<>();
                for(WarcRecord record : reader){
                    Map<String, List<String>> headers = record.headers().map();
                    if(headers.containsKey("WARC-Target-URI")
                            && headers.containsKey("WARC-Type")){
                        if(!headers.get("WARC-Target-URI").isEmpty()
                                && headers.get("WARC-Type").contains("response")){

                            //do something
                            bodyList.add(Text.getRecordBody(record));
                        }
                    }
                }
                return bodyList.iterator();
            });


            // Extract named entities
            JavaRDD<String> namedEntities = bodies.flatMap(text -> {
                //System.out.println(text);
                CoreNlp coreNlp = CoreNlp.getInstance();
                List<String> entities = coreNlp.findNamedEntities(text);
                return entities.iterator();
            });

            // Count the occurrences of each named entity
            Map<String, Long> entityCounts = namedEntities.countByValue();

            // Sort the entities by count in descending order
            List<Tuple2<String, Long>> sortedEntities = new ArrayList<>(entityCounts.entrySet())
                    .stream()
                    .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                    .sorted((e1, e2) -> e2._2().compareTo(e1._2()))
                    .limit(50)
                    .toList();

            // Print the top 50 named entities
            for (Tuple2<String, Long> entity : sortedEntities) {
                System.out.println(entity._1() + ": " + entity._2());
            }

            // Stop the SparkContext
            sc.stop();
        }

    }

}