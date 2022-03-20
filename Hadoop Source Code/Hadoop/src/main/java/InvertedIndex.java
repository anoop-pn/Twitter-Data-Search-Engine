//import java.io.IOException;
//import java.util.StringTokenizer;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Set;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Set;
//import java.util.regex.Pattern;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//
//
//public class WordCount {
//
//    public static class TokenizerMapper
//            extends Mapper<Object, Text, Text, IntWritable> {
//
//        private final static IntWritable one = new IntWritable(1);
//        private final Text word = new Text();
//
//        public void map(Object key, Text value, Context context
//        ) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
//        }
//    }
//
//    public static class IntSumReducer
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//        }
//    }
//
//
//
//    public static class MapTFIDF extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
//        private final static IntWritable one  = new IntWritable( 1);
//        private Text word  = new Text();
//
//        public void map( LongWritable offset,  Text lineText,  Context context)
//                throws  IOException,  InterruptedException {
//
//            String[] wordAndCounters = lineText.toString().split("\t");
//            String[] wordAndDoc = wordAndCounters[0].split("#####");
//            for (String str:wordAndDoc){
//                System.out.println(str);
//            }
//            context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
//
//        }
//    }
//
//    //Reducer for job2, gives the TFIDF score of the word as <word#####filename tfidfscore>
//    public static class ReduceTFIDF extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
//        @Override
//        public void reduce(Text key, Iterable<Text> value, Context context)
//                throws IOException, InterruptedException {
//
//            Configuration conf = context.getConfiguration();
//            //Get Total number of files
//            int totalfiles = conf.getInt("count", 0);
//
//            //HashMap to store filename as key and WFscore as its value
//            Map<String, String> filewf = new HashMap<String, String>();
//            // Calculate the occurace files containing key/word
//            int fileCointWord = 0;
//
//            for (Text wftext : value) {
//                fileCointWord++;
//                String[] wfstring = wftext.toString().split("=");
//                filewf.put(wfstring[0], wfstring[1]);
//            }
//
//
//            for (Map.Entry<String, String> entry : filewf.entrySet()) {
//
//                context.write(new Text(key + "#####" + entry.getKey()), new DoubleWritable(TFIDFValue(totalfiles, fileCointWord, Double.parseDouble(entry.getValue()))));
//            }
//
//        }
//
//        private double TFIDFValue(int totalfiles, int fileCointWord, Double wfscore) {
//            double tfidfscore;
//            tfidfscore = (Math.log10(1 + (totalfiles / fileCointWord))) * wfscore;
//
//            return tfidfscore;
//
//        }
//    }
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(WordCount.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        job.waitForCompletion(true);
//
//        Job job2  = Job .getInstance(conf, " wordcount ");
//        job2.setJarByClass( WordCount.class);
//        FileInputFormat.addInputPath(job2, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
//        job2.setMapperClass( MapTFIDF .class);
//        job2.setReducerClass( ReduceTFIDF .class);
//        job2.setOutputKeyClass( Text .class);
//        job2.setOutputValueClass( Text .class);
//        System.exit(job2.waitForCompletion(true) ? 0 : 1);    }
//}
//
import java.awt.List;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

import java.util.Set;


public class InvertedIndex extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(WordCount.class);

    private static final String OUTPUT_PATH = "intermediate_output";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        String filePath = args[2];


        //Job1 -> For WF calculations
//        Job job1  = Job .getInstance(getConf(), " wordcount ");
//        job1.setJarByClass( this .getClass());
//
//        FileInputFormat.addInputPaths(job1,  args[0]);
//        FileOutputFormat.setOutputPath(job1,  new Path("intermediate_output"));
//        job1.setMapperClass( MapTF .class);
//        job1.setReducerClass( ReduceTF .class);
//        job1.setOutputKeyClass( Text .class);
//        job1.setOutputValueClass( IntWritable .class);
//        job1.waitForCompletion(true);
//
//        Configuration jobConf = job1.getConfiguration();
//        //Get the total inputs in agr[0] i.e. Total number of files
//        FileSystem fs = FileSystem.get(jobConf);
//        int docNumber = fs.listStatus(new Path(args[0])).length;
//        jobConf.setInt("count", docNumber);
//
//        //Job2 -> For TFIDF calculations
//        Job job2  = Job .getInstance(jobConf, " wordcount ");
//        job2.setJarByClass( this .getClass());
//
//        FileInputFormat.setInputPaths(job2, new Path("intermediate_output"));
//        FileOutputFormat.setOutputPath(job2,  new Path("intermediate_output1"));
//        job2.setMapperClass( MapTFIDF .class);
//        job2.setReducerClass( ReduceTFIDF .class);
//        job2.setOutputKeyClass( Text .class);
//        job2.setOutputValueClass( Text .class);
//        job2.waitForCompletion(true);
//        //Set the configuration for the command line input for search query
//        jobConf.set("query",args[2]);
//        // Configure job 3 -> Search implementation
//        Job job3  = Job .getInstance(jobConf, " wordcount ");
//        job3.setJarByClass( this .getClass());
//
//        FileInputFormat.setInputPaths(job3, new Path("intermediate_output1"));
//        FileOutputFormat.setOutputPath(job3,  new Path(args[1]));
//        job3.setMapperClass( MapSearch .class);
//        job3.setReducerClass( ReduceSearch .class);
//        job3.setOutputKeyClass( Text .class);
//        job3.setOutputValueClass( Text .class);
//        job3.waitForCompletion(true) ;

        Job job4 = Job.getInstance(getConf(), " inverted ");
        job4.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job4, args[0]);
        FileOutputFormat.setOutputPath(job4, new Path("intermediate_output3"));
        job4.setMapperClass(MapTF.class);
        job4.setReducerClass(ReduceTF.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        return job4.waitForCompletion(true) ? 0 : 1;
    }

    //Mapper for job1
    public static class MapTF extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text nameKey = new Text();
        private Text fileNameValue = new Text();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            // Get the input file split for input file name
            //String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            System.out.println("############");
            String data = lineText.toString();
            String[] field = data.split(",", -1);
            String imageId = null;
            String tags = null;

            if (null != field) {
                imageId = field[1];
                String[] words = field[6].split("\\s+");
                int length = words.length - 1;
                for (int i = 1; i < words.length; i++) {
                    String word = words[i];
                    nameKey.set(word);
                    String val = imageId;
                    fileNameValue.set(val);
                    Text wordfilename = new Text(val + "#####" + nameKey);
                    context.write(wordfilename, one);
                }

            }
//
//            for ( String word  : WORD_BOUNDARY .split(line)) {
//                System.out.println(word);
//                if (word.isEmpty()) {
//                    continue;
//                }
//                currentWord  = new Text(word);
//                Text wordfilename = new Text(word+"#####"+fileName);
//                context.write(wordfilename, one);
//            }
//        }
        }
    }

    //
//    //Reducer for job1 which gives <word#####filename wordfrequency> as output
    public static class ReduceTF extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new DoubleWritable(WFvalue(sum)));
        }

        //
//        //Function to Calculate WF
        private double WFvalue(int sum) {
            double value = 0.0;
            if (sum > 0) {
                value = 1 + Math.log10(sum);
            } else {
                value = 0.0;
            }
            return value;
        }
    }
//
//    // Mapper for job2, gives <word filename=wordfrequency> as output to reducer
//    public static class MapTFIDF extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
//        private final static IntWritable one  = new IntWritable( 1);
//        private Text word  = new Text();
//
//        public void map( LongWritable offset,  Text lineText,  Context context)
//                throws  IOException,  InterruptedException {
//
//            String[] wordAndCounters = lineText.toString().split("\t");
//            String[] wordAndDoc = wordAndCounters[0].split("#####");
//            context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
//
//        }
//    }
//
//    //Reducer for job2, gives the TFIDF score of the word as <word#####filename tfidfscore>
//    public static class ReduceTFIDF extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
//        @Override
//        public void reduce( Text key,  Iterable<Text> value,  Context context)
//                throws IOException,  InterruptedException {
//
//            Configuration conf = context.getConfiguration();
//            //Get Total number of files
//            int totalfiles = conf.getInt("count",0);
//
//            //HashMap to store filename as key and WFscore as its value
//            Map<String, String> filewf = new HashMap<String, String>();
//            // Calculate the occurace files containing key/word
//            int fileCointWord = 0;
//            for(Text wftext : value){
//                fileCointWord++;
//                String[] wfstring = wftext.toString().split("=");
//                filewf.put(wfstring[0], wfstring[1]);
//            }
//
//
//            for(Map.Entry<String, String> entry : filewf.entrySet()){
//
//                context.write(new Text(key+"#####"+entry.getKey()),  new DoubleWritable(TFIDFValue(totalfiles,fileCointWord,Double.parseDouble(entry.getValue()))));
//            }
//
//        }
//        //Function to Calculate TDIDF
//        private double TFIDFValue(int totalfiles, int fileCointWord, Double wfscore){
//            double tfidfscore;
//            tfidfscore = (Math.log10(1 + (totalfiles / fileCointWord)))*wfscore;
//
//            return tfidfscore;
//
//        }
//    }
//
//    //Mapper for Job3
//    public static class MapSearch extends Mapper<Object ,  Text ,  Text ,  Text > {
//
//        private static ArrayList<String> queryList = new ArrayList<String>();
//        // Get input from command line from the configuration
//        public void setup(Context context)
//                throws IOException{
//            String query = context.getConfiguration().get("query");
//            String[] qeuryarr = query.split(" ");
//            for(String st : qeuryarr){
//
//                queryList.add(st);
//            }
//        }
//
//        public void map( Object key,  Text value,  Context context)
//                throws  IOException,  InterruptedException {
//            // Split the input to get word, file and TFIDF seperated
//            String[] wordFileAndTfidf = value.toString().split("\t");
//            String[] wordAndFile = wordFileAndTfidf[0].split("#####");
//            // String matching from search query
//            if(queryList.contains(wordAndFile[0])){
//                context.write(new Text(wordAndFile[1]), new Text(wordFileAndTfidf[1]));
//            }
//        }
//    }
//
//    //Reducer for job3 which gives <filename TFIDF> as output
//    public static class ReduceSearch extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
//        @Override
//        public void reduce( Text file,  Iterable<Text> tfidf,  Context context)
//                throws IOException,  InterruptedException {
//            Double sum  = 0.0;
//            for ( Text count  : tfidf) {
//                String s1 = count.toString();
//                sum  += Double.valueOf(s1);
//            }
//            context.write(file,  new DoubleWritable(sum));
//        }
//    }

//    public static class InvertedIndexNameMapper extends Mapper<LongWritable, Text, Text, Text> {
//        private Text nameKey = new Text();
//        private Text fileNameValue = new Text();
//        private final static IntWritable one  = new IntWritable(1);
//
//        public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("############");
//            String data = value.toString();
//            //System.out.println(data);
//            String[] field = data.split(",", -1);
//            String imageId = null;
//            String tags = null;
//            //System.out.println(field.length);
//
//            if (null != field ) {
//                imageId=field[1];
//                String[] words = field[6].split("\\s+");
//                int length = words.length - 1;
//                for(int i=1; i< words.length; i++)
//                {
//                    String word = words[i];
//                    nameKey.set(word);
//                    String val = imageId ;
//                    fileNameValue.set(val);
//                    context.write(nameKey, fileNameValue);
//                }
//
//            }
//            System.out.println("############");
//        }
//    }
//    //test reducer
//    public static class InvertedIndexNameReducer extends Reducer<Text, Text, Text, Text> {
//        private Text result = new Text();
//
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            //System.out.println(" ^^^ "+ key);
//            StringBuilder sb = new StringBuilder();
//            boolean first = true;
//            for (Text value : values) {
//
//                if (first) {
//                    first = false;
//                } else {
//                    sb.append(" ");
//                }
//                if (sb.lastIndexOf(value.toString()) < 0) {
//                    sb.append(value.toString());
//                }
//            }
//            result.set(sb.toString());
//            int sum  = 0;
//           for ( IntWritable count  : counts) {
//                sum  += count.get();
//            }
////            context.write(word,  new DoubleWritable(WFvalue(sum)));
//            context.write(key, result);
//        }
//
//    }


}
