import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;



public class WordCount extends Configured implements Tool {

    private static final Logger LOG = Logger .getLogger( WordCount.class);

    private static final String OUTPUT_PATH = "intermediate_output";

    public static void wordcountRunner(String [] args) throws  Exception {
        long start=System.currentTimeMillis();
        int res  = ToolRunner .run( new WordCount(), args);
        long end=System.currentTimeMillis();
        System.out.println("Time==" + (end-start));

    }

    public int run( String[] args) throws  Exception {
        //Job1 -> For WF calculations
        Job job1  = Job .getInstance(getConf(), " wordcount ");
        job1.setJarByClass( this .getClass());
        FileInputFormat.addInputPaths(job1, args[0]);
        FileOutputFormat.setOutputPath(job1,  new Path("intermediate_output"));
        job1.setMapperClass( MapTF .class);
        job1.setReducerClass( ReduceTF .class);
        job1.setOutputKeyClass( Text .class);
        job1.setOutputValueClass( IntWritable .class);
        job1.waitForCompletion(true);

        Configuration jobConf = job1.getConfiguration();
        FileSystem fs = FileSystem.get(jobConf);
        int docNumber = fs.listStatus(new Path(args[0])).length;
        jobConf.setInt("count", docNumber);

        //Job2 -> For TFIDF calculations
        Job job2  = Job .getInstance(jobConf, " wordcount ");
        job2.setJarByClass( this .getClass());

        FileInputFormat.setInputPaths(job2, new Path("intermediate_output"));
        FileOutputFormat.setOutputPath(job2,  new Path("intermediate_output1"));
        job2.setMapperClass( MapTFIDF .class);
        job2.setReducerClass( ReduceTFIDF .class);
        job2.setOutputKeyClass( Text .class);
        job2.setOutputValueClass( Text .class);
        return job2.waitForCompletion(true)?0:1;
    }

    //Mapper for job1
    public static class MapTF extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
        private final static IntWritable one  = new IntWritable(1);
        private Text word  = new Text();
        private Text tweetID = new Text();

        public void map( LongWritable offset,  Text lineText,  Context context)
                throws  IOException,  InterruptedException {
            String data = lineText.toString();
            String[] field = data.split(",", -1);
            String tweetId = null;
            if (null != field) {
                tweetId=field[0];
                String[] wordsSet = field[1].split("\\s+");
                int length = wordsSet.length - 1;
                for(int i=1; i< wordsSet.length; i++)
                {
                    String words = wordsSet[i];
                    word.set(words);
                    String val = tweetId;
                    tweetID.set(val);
                    Text wordfilename = new Text(words+","+val);
                    context.write(wordfilename, one);
                }

            }

        }
    }

    //Reducer for job1 which gives <word#####filename wordfrequency> as output
    public static class ReduceTF extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
        @Override
        public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
                throws IOException,  InterruptedException {
            int sum  = 0;
            for ( IntWritable count  : counts) {
                sum  += count.get();
            }
            context.write(word,  new DoubleWritable(WFvalue(sum)));
        }

        //Function to Calculate WF
        private double WFvalue(int sum){
            double value = 0.0;
            if(sum > 0){
                value = 1 + Math.log10(sum);
            }
            else {
                value = 0.0;
            }
            return value;
        }
    }

    // Mapper for job2, gives <word filename=wordfrequency> as output to reducer
    public static class MapTFIDF extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
        private final static IntWritable one  = new IntWritable( 1);
        private Text word  = new Text();

        public void map( LongWritable offset,  Text lineText,  Context context)
                throws  IOException,  InterruptedException {
            String[] wordAndCounters = lineText.toString().split("\t");
            String[] wordAndDoc = wordAndCounters[0].split(",");
            context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));

        }
    }

    //Reducer for job2, gives the TFIDF score of the word as <word#####filename tfidfscore>
    public static class ReduceTFIDF extends Reducer<Text ,  Text ,  Text ,  Text > {
        @Override
        public void reduce( Text key,  Iterable<Text> value,  Context context)
                throws IOException,  InterruptedException {

            Configuration conf = context.getConfiguration();
            //Get Total number of files
            int totalfiles = conf.getInt("count",0);

            //HashMap to store TweetId and word as key and WFscore as its value
            Map<String, String> filewf = new HashMap<String, String>();
            // Calculate the occurace files containing key/word
            int fileCountWord = 0;
            for(Text wftext : value){
                fileCountWord++;
                String[] wfstring = wftext.toString().split("=");
                filewf.put(wfstring[0], wfstring[1]);
            }


            for(Map.Entry<String, String> entry : filewf.entrySet()){

                context.write(new Text(key+","+entry.getKey()), new Text(TFIDFValue(totalfiles,fileCountWord,Double.parseDouble(entry.getValue()))));
            }

        }
        //Function to Calculate TDIDF
        private String TFIDFValue(int totalfiles, int fileCountWord, Double wfscore){
            Double  tfidfscore;
            tfidfscore = (Math.log10(1 + (totalfiles / fileCountWord)))*wfscore;
            return ","+ tfidfscore.toString() ;

        }
    }

}