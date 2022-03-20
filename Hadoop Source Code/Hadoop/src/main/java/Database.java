
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Database extends Configured {
    public static void main(String[] args) throws Exception {
        WordCount wordcount=new WordCount();
        wordcount.wordcountRunner(args);
        String filepath="/Users/nadiasaba/IdeaProjects/Hadoop/intermediate_output1/part-r-00000";
        Map<String, List<List<String>>> result=invertedIndexMapGenerator(filepath);
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
            if(resultMap.get(word) == null){
                List<List<String>> temp = new ArrayList<>();
                temp.add(Arrays.asList(new String[]{score,tweetId}));
                resultMap.put(word, temp);
            } else {
                List<List<String>> temp = resultMap.get(word);
                temp.add(Arrays.asList(new String[]{score,tweetId}));
                temp.sort(Comparator.comparing(l -> l.get(0)));
                Collections.reverse(temp);
                resultMap.put(word, temp);
            }
        }
        return resultMap;
    }

//    public static Map<String,List<List<String>>> dbInsertion(){
//       Mongo mongo = new Mongo("localhost", 27017);
//       DB db = mongo.getDB("IRDB");
//       DBCollection collection = db.getCollection("dummyColl");
//       Map<String, List<List<String>>> inMap =  new HashMap<>();
//       List<Document> documents = new ArrayList<>();
//       for(Map.Entry<String, List<List<String>>> kv :inMap.entrySet()) {
//            Document doc = new Document();
//           doc.put("_id", kv.getKey());
//           List<List<String>> values = kv.getValue();
//           doc.put("query", values.get(0));
//            documents.add(doc);
//       }
//        collection;
//    }
}
