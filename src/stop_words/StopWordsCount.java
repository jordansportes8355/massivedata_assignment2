package stop_words;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StopWordsCount extends Configured implements Tool {
   public static void main(String[] args) throws Exception {

      int res = ToolRunner.run(new Configuration(), new StopWordsCount(), args);
      
      System.exit(res);
   }
   

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = getConf();
      conf.set("mapred.textoutputformat.separatorText", ",");
      Job job = new Job(conf, "StopWordCount");
      job.setNumReduceTasks(1);
      job.setJarByClass(StopWordsCount.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

     
      
      FileInputFormat.addInputPath(job, new Path("input/pg100.txt")); 
      Path outputPath = new Path("OutputStopWordsCount");
      FileOutputFormat.setOutputPath(job, outputPath);
      FileSystem hdfs = FileSystem.get(getConf());
	  if (hdfs.exists(outputPath))
	      hdfs.delete(outputPath, true);

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
         for (String token: value.toString().split("\\s+")) {
        	token = token.replaceAll("[^a-zA-Z ]", "").toLowerCase();
            word.set(token);
            context.write(word, ONE);
         }
      }
   }


   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += 1;
         }
         if(sum>4000)
        	 context.write(key, new IntWritable(sum));
      }
   }
}