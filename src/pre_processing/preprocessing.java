package pre_processing;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.*;
import java.util.regex.Pattern;
import java.net.URI;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class preprocessing extends Configured implements Tool {

	public static enum counters {
		nb_lines,
	};

   public static void main(String[] args) throws Exception {

      int res = ToolRunner.run(new Configuration(), new preprocessing(), args);
      
      System.exit(res);
   }
   

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = getConf();
      conf.set("mapred.textoutputformat.separatorText", ",");
      Job job = new Job(conf, "preprocessing");
      job.setNumReduceTasks(1);
      job.setJarByClass(this.getClass());
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.getConfiguration().setBoolean("stop_word", true);
	  job.addCacheFile(new Path(args[0]).toUri());
	  
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

     
      
      FileInputFormat.addInputPath(job, new Path("input/pg100.txt")); 
      Path outputPath = new Path("input_preprocessing");
      FileOutputFormat.setOutputPath(job, outputPath);
      FileSystem hdfs = FileSystem.get(getConf());
	  if (hdfs.exists(outputPath))
	      hdfs.delete(outputPath, true);

      job.waitForCompletion(true);
      
      long counter = job.getCounters().findCounter(counters.nb_lines).getValue();
		Path outFile = new Path("nb_lines.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(outFile, true)));
		br.write(String.valueOf(counter));
		br.close();
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
	  
	   private Set<String> WordToSkip = new HashSet<String>();
		private BufferedReader buff;

		protected void setup(Mapper.Context context) throws IOException,
				InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
			if (config.getBoolean("stop_word", false)) {
				URI[] localPaths = context.getCacheFiles();
				parsing(localPaths[0]);
			}
		}

		private void parsing(URI uri) {
			try {
				buff = new BufferedReader(new FileReader(new File(
						uri.getPath()).getName()));
				String pattern;
				while ((pattern = buff.readLine()) != null) {
					WordToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while parsing the cached file '" + uri + "' : " + StringUtils.stringifyException(ioe));
			}
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			for (String word : value.toString().split("\\s*\\b\\s*")) {

				Pattern pattern = Pattern.compile("[^A-Za-z0-9]");

				if (value.toString().length() == 0
						|| word.toLowerCase().isEmpty()
						|| WordToSkip.contains(word.toLowerCase())
						|| pattern.matcher(word.toLowerCase()).find()) {
					continue;
				}

				context.write(key, new Text(word.toLowerCase()));
			}
		}
	}


   public static class Reduce extends
	Reducer<LongWritable, Text, LongWritable, Text> {

private BufferedReader reader;

@Override
public void reduce(LongWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

	ArrayList<String> wordslist = new ArrayList<String>();

	HashMap<String, String> wordcount = new HashMap<String, String>();
	reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/Assignment2/outputwordwount/part-r-00000")));
	String pattern;
	while ((pattern = reader.readLine()) != null) {
		String[] word = pattern.split(",");
		wordcount.put(word[0], word[1]);
	}

	for (Text word : values) {
		wordslist.add(word.toString());
	}

	HashSet<String> wordsHS = new HashSet<String>(wordslist);

	StringBuilder wordswithcountSB = new StringBuilder();

	String firstprefix = "";
	for (String word : wordsHS) {
		wordswithcountSB.append(firstprefix);
		firstprefix = ", ";
		wordswithcountSB.append(word + "#" + wordcount.get(word));
	}

	java.util.List<String> wordswithcountL = Arrays
			.asList(wordswithcountSB.toString().split("\\s*,\\s*"));

	Collections.sort(wordswithcountL, new Comparator<String>() {
		public int compare(String o1, String o2) {
			return extractInt(o1) - extractInt(o2);
		}

		int extractInt(String s) {
			String num = s.replaceAll("[^#\\d+]", "");
			num = num.replaceAll("\\d+#", "");
			num = num.replaceAll("#", "");
			return num.isEmpty() ? 0 : Integer.parseInt(num);
		}
	});

	StringBuilder wordswithcountsortedSB = new StringBuilder();

	String secondprefix = "";
	for (String word : wordswithcountL) {
		wordswithcountsortedSB.append(secondprefix);
		secondprefix = ", ";
		wordswithcountsortedSB.append(word);
	}

	context.getCounter(counters.nb_lines).increment(1);

	//context.write(key, new Text(wordswithcountsortedSB.toString()));
	context.write(key, new Text(wordswithcountsortedSB.toString().replaceAll("#\\d+", "")));

}
}
}