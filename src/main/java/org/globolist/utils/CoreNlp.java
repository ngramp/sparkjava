package org.globolist.utils;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CoreNlp implements Serializable {
    private final StanfordCoreNLP pipeline;
    private static CoreNlp INSTANCE;
    private CoreNlp(Properties props){
        this.pipeline = new StanfordCoreNLP(props);
    }
    public static CoreNlp getInstance() {
        if(INSTANCE == null) {
            Properties props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner");
            INSTANCE = new CoreNlp(props);
        }
        return INSTANCE;
    }
    public List<String> findNamedEntities(String text) {
        // Initialize StanfordNLP pipeline
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);
        return getEntityList(sentences);
    }
    private List<String> getEntityList(List<CoreMap> sentences){
        List<String> entities = new ArrayList<>();
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                String namedEntityTag = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                String namedEntity = token.word();
                if (!namedEntityTag.equals("O")) {
                    entities.add(namedEntity);
                }
            }
        }
        System.out.println(entities);
        return entities;
    }
    public void loadModels(JavaSparkContext sc){
        String version = "4.5.4";
        String baseUrl = "http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp";
        String model = "stanford-corenlp-" + version + "-models.jar";
        String url = baseUrl + "/" + version + "/" + model;

        ProcessBuilder processBuilder = new ProcessBuilder("wget", "-N", url);
        try{
            Process process = processBuilder.start();
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line1;
            while ((line1 = reader1.readLine()) != null) {
                System.out.println(line1);
            }
            processBuilder = new ProcessBuilder("jar", "xf", model);
            process = processBuilder.start();
            BufferedReader reader2 = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line2;
            while ((line2 = reader2.readLine()) != null) {
                System.out.println(line2);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Assuming 'sc' is an instance of SparkContext
        // add model to workers
        sc.addJar(model);
    }
}
