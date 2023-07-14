package org.globolist;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Main {
    public static void main(String[] args) {
        try (JavaSparkContext sc = MySpark.createJSC()) {
            // Read WARC-GZ files from the input directory
            JavaPairRDD<String, PortableDataStream> warcFilesRDD = sc.binaryFiles("/home/gram/Downloads/*.warc.gz");
            // Extract TLDs and count unique TLDs
            JavaPairRDD<String, Integer> uniqueTLDsCount = warcFilesRDD.flatMapToPair(file -> {
                WarcReader reader = new WarcReader(file._2().open());
                Set<String> domains = new HashSet<>();

                for(WarcRecord record : reader){
                    Map<String, List<String>> headers = record.headers().map();
                    if(headers.containsKey("WARC-Target-URI")
                            && headers.containsKey("WARC-Type")){
                        if(!headers.get("WARC-Target-URI").isEmpty()
                                && headers.get("WARC-Type").contains("response")){
                            domains.add(headers.get("WARC-Target-URI").get(0));
                        }
                    }
                }
                return domains.stream()
                        .map(domain -> {
                            String tld = extractTLD(domain);
                            return new Tuple2<>(tld, 1);
                        })
                        .iterator();
            }).reduceByKey(Integer::sum);

            // Swap the key-value pairs and sort in descending order based on count
            // Sort the TLDs by count in descending order
            JavaPairRDD<Integer, String> sortedTLDs = uniqueTLDsCount.mapToPair(Tuple2::swap)
                    .sortByKey(false);

            // Take the top ten TLDs
            JavaPairRDD<Integer, String> topTenTLDs = sc.parallelizePairs(sortedTLDs.take(10));

            // Print the top ten TLDs
            topTenTLDs.foreach(pair -> System.out.println(pair._2 + ": " + pair._1));

            // Stop the context
            sc.stop();

        }

    }

    private static String extractTLD(String domain) {
        String[] domainParts = domain.split("\\.");
        if (domainParts.length > 1) {
            return domainParts[1];
        }
        return "";
    }
}