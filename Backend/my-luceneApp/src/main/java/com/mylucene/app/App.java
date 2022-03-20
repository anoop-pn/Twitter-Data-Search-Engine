package com.mylucene.app;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

@RestController
@EnableAutoConfiguration
@SpringBootApplication
public class App {

    static String fp=new String();
    static Map<String, List<List<String>>> invertedMap = new HashMap<>();
    @CrossOrigin(origins = "*")
    @RequestMapping("/")
    String home() {
        return "Backend for twitter Search up and running!!!!";
    }

    @CrossOrigin(origins = "*", allowedHeaders = "*")
    @RequestMapping(value = "luceneIndex", method = RequestMethod.GET)
    @ResponseBody
    JSONObject luceneSearch(@RequestParam String searchQuery)throws IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException, org.json.simple.parser.ParseException {
        String filePath=fp;
        String searchString=searchQuery;
        Analyzer analyzer = new StandardAnalyzer();
        Directory directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter indexWriter = new IndexWriter(directory, config);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject =  (JSONObject)jsonParser.parse(new InputStreamReader(new FileInputStream(filePath)));
        Set<String> val=jsonObject.keySet();
        Iterator<String> iterator=val.iterator();
        int count=0;
        Long currentTime=System.currentTimeMillis();
        while(iterator.hasNext()) {
            count++;
            if(count%5000==0) {
                System.out.println(count+","+(System.currentTimeMillis()-currentTime));
            }
            JSONObject obj=(JSONObject)jsonObject.get(iterator.next());
            Document doc = new Document();
            doc.add(new TextField("id", obj.get("id").toString(), Field.Store.YES));
            doc.add(new TextField("created_at", obj.get("created_at").toString(), Field.Store.NO));
            JSONArray hashTags=(JSONArray) obj.get("hashtags");
            if(hashTags!=null) {
                Iterator hashTagsit=hashTags.iterator();
                while(hashTagsit.hasNext()) {
                    String cleanedHashtTags=hashTagAnalyserr(hashTagsit.next().toString());
                    doc.add(new TextField("hashtags",cleanedHashtTags, Field.Store.YES));
                }
            }
            JSONArray place=(JSONArray) obj.get("place");
            if(place!=null) {
                Iterator placeit=place.iterator();
                while(placeit.hasNext()) {
                    doc.add(new TextField("place",placeit.next().toString() , Field.Store.NO));
                }
            }
            doc.add(new TextField("screen_name", obj.get("screen_name").toString(), Field.Store.YES));
            doc.add(new TextField("user_location", obj.get("user_location").toString(), Field.Store.YES));
            //String cleanedData=textAnalyser(obj.get("text").toString());
            doc.add(new TextField("text", obj.get("text").toString(), Field.Store.YES));
            indexWriter.addDocument(doc);
        }
        indexWriter.close();
        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        QueryParser parser=new QueryParser("text", new StandardAnalyzer());
        Query query = parser.parse(searchString);
        int topHitCount = 100;
        ScoreDoc[] hits = indexSearcher.search(query, topHitCount).scoreDocs;
        JSONObject returnObj = new JSONObject();
        for (int rank = 0; rank < Math.min(hits.length, 10); ++rank) {
            Document hitDoc = indexSearcher.doc(hits[rank].doc);
            System.out.println((rank + 1) + " (score:" + hitDoc.get("id") + ") --> " + hitDoc.get("text"));
            JSONObject rankScoreText = new JSONObject();
            rankScoreText.put("rank",rank+1);
            rankScoreText.put("score",hits[rank].score);
            rankScoreText.put("text",hitDoc.get("text"));
            if(!rankScoreText.isEmpty()) {
                returnObj.put(rank,rankScoreText);
            }
        }
        indexReader.close();
        directory.close();
        return returnObj;
    }

    @CrossOrigin(origins = "*", allowedHeaders = "*")
    @RequestMapping(value = "hadoopIndex", method = RequestMethod.GET)
    @ResponseBody
    JSONObject hadoopSearch(@RequestParam String searchQuery)throws IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException, org.json.simple.parser.ParseException {
        JSONObject returnObj = new JSONObject();
        List<List<String>> res=invertedMap.get(searchQuery);
        for (int rank = 0; rank < Math.min(res.size(), 10); ++rank) {
            System.out.println("Entering here");
            JSONObject rankScoreText = new JSONObject();
            rankScoreText.put("rank",rank+1);
            rankScoreText.put("score",res.get(rank).get(0));
            String twitterText=res.get(rank).get(2);
            rankScoreText.put("text",twitterText);
            if(!rankScoreText.isEmpty()) {
                returnObj.put(rank,rankScoreText);
            }
            System.out.print("RANK: "+rank+1);
            System.out.print(" SCORE: "+res.get(rank));
            System.out.println(" TEXT: "+twitterText);
        }
        return returnObj;
    }

    private static String hashTagAnalyserr(String string) throws IOException {
    List<String> list=new ArrayList<String>();
    Analyzer analyzer= new StandardAnalyzer();
    TokenStream tokenStream= analyzer.tokenStream("text", string);
    CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while(tokenStream.incrementToken()) {
        //System.out.print("[" + term.toString() + "] ");
        list.add(term.toString());
    }
    tokenStream.close();
    analyzer.close();
    return String.join(" ",list);
    }

    public static Map<String, List<List<String>>> invertedIndexMapGenerator(String filePath){

        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String,List<List<String>>> resultMap = new HashMap<>();

        for(List<String> row : records){
            String word = row.get(0);
            String tweetId = row.get(1);
            String score = row.get(2);
            String tweetText = row.get(3);
            if(resultMap.get(word) == null){
                List<List<String>> temp = new ArrayList<>();
                temp.add(Arrays.asList(new String[]{score.trim(),tweetId.trim().replace("\"", ""),tweetText}));
                resultMap.put(word, temp);
            } else {
                List<List<String>> temp = resultMap.get(word);
                temp.add(Arrays.asList(new String[]{score.trim(),tweetId.trim().replace("\"", ""),tweetText}));
                temp.sort(Comparator.comparing(l -> l.get(0)));
                Collections.reverse(temp);
                resultMap.put(word, temp);
            }
        }
        return resultMap;
    }
    public static void main(String[] args) throws IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException, org.json.simple.parser.ParseException {
        fp=args[0];
        invertedMap=invertedIndexMapGenerator(args[1]);
        SpringApplication.run(App.class, args);
    }
}