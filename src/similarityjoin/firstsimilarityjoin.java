package similarityjoin;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.WritableComparable;

class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	public TextPair(Text first, Text second) {
		set(first, second);
	}

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public String toString() {
		return first + " " + second;
	}

	@Override
	public int compareTo(TextPair other) {
		int firstfirst = first.compareTo(other.first);
		int secondsecond = second.compareTo(other.second);
		int firstsecond = first.compareTo(other.second);
		int secondfirst = second.compareTo(other.first);

		if (firstfirst == 0 && secondsecond == 0 || firstsecond == 0 && secondfirst == 0) {
			return 0;
		}

		Text smaller1;
		Text smaller2;

		Text bigger1;
		Text bigger2;

		if (this.first.compareTo(this.second) < 0) {
			smaller1 = this.first;
			bigger1 = this.second;
		} else {
			smaller1 = this.second;
			bigger1 = this.first;
		}

		if (other.first.compareTo(other.second) < 0) {
			smaller2 = other.first;
			bigger2 = other.second;
		} else {
			smaller2 = other.second;
			bigger2 = other.first;
		}

		int smaller1smaller2 = smaller1.compareTo(smaller2);
		int bigger1bigger2 = bigger1.compareTo(bigger2);

		if (smaller1smaller2 == 0) {
			return bigger1bigger2;
		} else {
			return smaller1smaller2;
		}
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

}

public class firstsimilarityjoin extends Configured implements Tool {

		public static enum counters {
			nb_comparaison,
		};

		public static void main(String[] args) throws Exception {

		      int res = ToolRunner.run(new Configuration(), new firstsimilarityjoin(), args);
		      
		      System.exit(res);
		   }

		@Override
		public int run(String[] args) throws Exception {
		     Configuration conf = getConf();
		     conf.set("mapred.textoutputformat.separatorText", ",");
		     Job job = new Job(conf, "firstsimilarityjoin");

			job.setJarByClass(firstsimilarityjoin.class);
			FileInputFormat.addInputPath(job, new Path("input_preprocessing/part-r-00000"));
			Path outputPath = new Path("OutputFirstSim");
			FileOutputFormat.setOutputPath(job, outputPath);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setMapOutputKeyClass(TextPair.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.getConfiguration().set(
					"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
					",");
			job.getConfiguration().set(
					"mapreduce.output.textoutputformat.separator", ", ");
			job.setNumReduceTasks(1);

			FileSystem fs = FileSystem.newInstance(getConf());

			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			job.waitForCompletion(true);
			long counter = job.getCounters()
					.findCounter(counters.nb_comparaison).getValue();
			Path outFile = new Path("nb_comparaison1,txt");
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
					fs.create(outFile, true)));
			br.write(String.valueOf(counter));
			br.close();
			return 0;
		}

		public static class Map extends Mapper<Text, Text, TextPair, Text> {

			private BufferedReader reader;
			private static TextPair textPair = new TextPair();

			@Override
			public void map(Text key, Text value, Context context)
					throws IOException, InterruptedException {

				HashMap<String, String> linesHP = new HashMap<String, String>();
				reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/Assignment2/input_preprocessing/part-r-00000")));
				String pattern;
				while ((pattern = reader.readLine()) != null) {
					//System.out.println(pattern);
					//System.out.println(pattern.split("\t")[0]);
					//System.out.println(pattern.split("\t")[1]);
					String[] line = pattern.split("\t");
					linesHP.put(line[0], line[1].replaceAll(",", ""));
					//System.out.println(line[0].split("#")[0]);
					//System.out.println(linesHP);
					//System.out.println (linesHP.keySet());
				}

				for (String line : linesHP.keySet()) {
					
					if (key.toString().equals(line)) {
						continue;
					}
					
					textPair.set(key, new Text(line));
					context.write(textPair, new Text(value.toString()));
				}
			}
		}

		public static class Reduce extends Reducer<TextPair, Text, Text, Text> {

			private BufferedReader reader;

			public double jaccard(TreeSet<String> ts1, TreeSet<String> ts2) {

				if (ts1.size() < ts2.size()) {
					TreeSet<String> ts1cp = ts1;
					ts1cp.retainAll(ts2);
					int inter = ts1cp.size();
					ts1.addAll(ts2);
					int union = ts1.size();
					return (double) inter / union;
				} else {
					TreeSet<String> ts2cp = ts2;
					ts2cp.retainAll(ts1);
					int inter = ts2cp.size();
					ts2.addAll(ts1);
					int union = ts2.size();
					return (double) inter / union;
				}

			}

			@Override
			public void reduce(TextPair key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {

				HashMap<String, String> linesHP = new HashMap<String, String>();
				reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/Assignment2/input_preprocessing/part-r-00000")));
				String pattern;
				while ((pattern = reader.readLine()) != null) {
					String[] line = pattern.split("\t");
					System.out.println(line[0]);
					linesHP.put(line[0], line[1].replaceAll(",", ""));
				}

				TreeSet<String> otherwords = new TreeSet<String>();
				String words = linesHP.get(key.getSecond().toString());
				for (String word : words.split(" ")) {
					otherwords.add(word);
				}

				TreeSet<String> wordsTS = new TreeSet<String>();

				for (String word : values.iterator().next().toString().split(" ")) {
					wordsTS.add(word);
				}

				context.getCounter(counters.nb_comparaison).increment(1);
				double sim = jaccard(wordsTS, otherwords);

				if (sim >= 0.8) {
					context.write(new Text("(" + key.getFirst() + ", " + key.getSecond() + ")"),new Text(String.valueOf(sim)));
				}
			}
		}
	}

