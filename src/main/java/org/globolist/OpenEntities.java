package org.globolist;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.netpreserve.jwarc.MessageBody;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Safelist;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class Main2 {
    public static void main(String[] args) {
        String nerfile = args[0];

        // Create a JavaSparkContext
        try(JavaSparkContext sc = createSC()) {

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

                            namedEntities.addAll(findNamedEntities(nerfile, record));
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

    private static List<String> findNamedEntities(String nerfile, WarcRecord record) {
        List<String> entitiesList = new ArrayList<>();

        try {
            // Load the OpenNLP models
            //TokenizerModel tokenizerModel = new TokenizerModel(new FileInputStream("path/to/tokenizer/model")); // Update with your tokenizer model path
            NameFinderME nameFinder = new NameFinderME(new TokenNameFinderModel(new FileInputStream(SparkFiles.get(nerfile)))); // Update with your NER model path

            // Tokenize the text
            Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
            String[] tokens;
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
                String cleanedHtml = cleanedDoc.body().html();
                tokens = tokenizer.tokenize(cleanedHtml);
            }

            // Find named entities
            Span[] spans = nameFinder.find(tokens);
            String[] entities = Span.spansToStrings(spans, tokens);
            if(entities.length > 0)
                System.out.println(Arrays.toString(entities));
            nameFinder.clearAdaptiveData();

            // Add named entities to the list
            entitiesList.addAll(Arrays.asList(entities));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return entitiesList;
    }

}