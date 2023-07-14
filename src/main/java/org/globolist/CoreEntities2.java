package org.globolist;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.globolist.utils.MySpark;
import org.globolist.utils.Text;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;

import java.io.File;
import java.io.IOException;

public class CoreEntities2 {
    public static void main(String[] args) {

        // Create a JavaSparkContext
        try(JavaSparkContext sc = MySpark.createJSC()) {
            String inputDirPath = "/home/gram/Downloads/";
            File inputDir = new File(inputDirPath);
            File[] files = inputDir.listFiles((dir, name) -> name.endsWith(".warc.gz"));
            // Process files one by one with maximum parallelism for records
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        System.out.println("Opening " + file.getPath() + " with warcreader");
                        try(WarcReader reader = new WarcReader(file.toPath())){

                            System.out.println("Parrellising records");
                            System.out.println(reader.records().toList());
                            JavaRDD<WarcRecord> records = sc.parallelize(reader.records().toList());
                            System.out.println(records.collect());

                            System.out.println("Processing records");
                            JavaRDD<String> texts = records.map(record -> {return Text.getRecordBody(record);});
                            //System.out.println("uppercasing");
                            //JavaRDD<String> processedRecords = texts.map(String::toUpperCase);
                            System.out.println(texts.collect());


                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }


//            // Extract named entities
//            JavaRDD<String> namedEntities = bodies.flatMap(text -> {
//                //System.out.println(text);
//                CoreNlp coreNlp = CoreNlp.getInstance();
//                List<String> entities = coreNlp.findNamedEntities(text);
//                return entities.iterator();
//            });
//
//            // Count the occurrences of each named entity
//            Map<String, Long> entityCounts = namedEntities.countByValue();
//
//            // Sort the entities by count in descending order
//            List<Tuple2<String, Long>> sortedEntities = new ArrayList<>(entityCounts.entrySet())
//                    .stream()
//                    .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
//                    .sorted((e1, e2) -> e2._2().compareTo(e1._2()))
//                    .limit(50)
//                    .toList();
//
//            // Print the top 50 named entities
//            for (Tuple2<String, Long> entity : sortedEntities) {
//                System.out.println(entity._1() + ": " + entity._2());
//            }

            // Stop the SparkContext
            sc.stop();
        }

    }

}