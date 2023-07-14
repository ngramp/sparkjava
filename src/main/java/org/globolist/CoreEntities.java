package org.globolist;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Safelist;
import org.netpreserve.jwarc.MessageBody;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class Main3 {
    public static void main(String[] args) {
        String nerfile = args[0];

        // Create a JavaSparkContext
        try(JavaSparkContext sc = createSC()) {

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
                            bodyList.add(getRecordBody(record));
                        }
                    }
                }
                return bodyList.iterator();
            });
            // Extract named entities
            JavaRDD<String> namedEntities = bodies.flatMap(text -> {
                System.out.println(text);
                List<String> entities = findNamedEntities(text);
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

    private static String getRecordBody(WarcRecord record) {
        String cleanedHtml = "";
        try(MessageBody body = record.body()) {
            // Parse HTML using Jsoup
            Scanner s = new Scanner(body.stream()).useDelimiter("\\A");
            String result = s.hasNext() ? s.next() : "";
            Document doc = Jsoup.parse(result);

            // Remove script tags
            doc.select("script").remove();

            // Clean HTML using Safelist
            Safelist safelist = Safelist.basic();
            safelist.removeTags("b"); // Allow only 'b' tags
            Cleaner cleaner = new Cleaner(safelist);
            Document cleanedDoc = cleaner.clean(doc);
            //System.out.println(cleanedDoc);
            // Get the cleaned HTML
            cleanedHtml = cleanedDoc.body().html();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cleanedHtml;
    }

    private static List<String> findNamedEntities(String text) {
        List<String> entities = new ArrayList<>();
        // Initialize StanfordNLP pipeline
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);

        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                String namedEntity = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                if (!namedEntity.equals("O")) {
                    entities.add(namedEntity);
                }
            }
        }
        System.out.println(entities);
        return entities;
    }

    private static JavaSparkContext createSC() {
        SparkConf conf = new SparkConf().setAppName("My application")
                .setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

}