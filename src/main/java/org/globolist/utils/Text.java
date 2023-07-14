package org.globolist.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Safelist;
import org.netpreserve.jwarc.MessageBody;
import org.netpreserve.jwarc.WarcRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Scanner;

public class Text implements Serializable {
    public static String getRecordBody(WarcRecord record) {
        String cleanedHtml = "";
        try(MessageBody body = record.body()) {
            // Parse HTML using Jsoup
            Scanner s = new Scanner(body.stream()).useDelimiter("\\A");
            String result = s.hasNext() ? s.next() : "";
            //System.out.println(result);
            Document doc = Jsoup.parse(result);

            // Remove script tags
            doc.select("script").remove();
            doc.select("meta").remove();
            doc.select("a").remove();

            // Clean HTML using Safelist
            Safelist safelist = Safelist.none();
            //safelist.removeTags("b"); // Allow only 'b' tags
            Cleaner cleaner = new Cleaner(safelist);
            Document cleanedDoc = cleaner.clean(doc);
            //System.out.println(cleanedDoc);
            // Get the cleaned HTML
            cleanedHtml = cleanedDoc.body().text();
            //System.out.println(cleanedHtml);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cleanedHtml;
    }
}
