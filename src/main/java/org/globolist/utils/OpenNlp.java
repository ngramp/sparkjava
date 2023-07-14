package org.globolist.utils;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.Span;
import org.apache.spark.SparkFiles;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OpenNlp implements Serializable {
    public static List<String> findNamedEntities(String nerfile, String text) {
        List<String> entitiesList = new ArrayList<>();

        try {
            // Load the OpenNLP models
            //TokenizerModel tokenizerModel = new TokenizerModel(new FileInputStream("path/to/tokenizer/model")); // Update with your tokenizer model path
            NameFinderME nameFinder = new NameFinderME(new TokenNameFinderModel(new FileInputStream(SparkFiles.get(nerfile)))); // Update with your NER model path

            // Tokenize the text
            Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
            String[] tokens = tokenizer.tokenize(text);
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
