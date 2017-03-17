package similarityjoin;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class secondsimilarityjoin extends Configured implements Tool {

	public static enum counters {
		nb_comparaison,
	};

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new secondsimilarityjoin(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = Job.getInstance(getConf(), "secondsimilarityjoin");

		job.setJarByClass(secondsimilarityjoin.class);
		FileInputFormat.addInputPath(job, new Path("input_preprocessing/part-r-00000"));
		Path outputPath = new Path("OutputSecondSim");
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ", ");
		job.setNumReduceTasks(1);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.waitForCompletion(true);
		long counter = job.getCounters()
				.findCounter(counters.nb_comparaison).getValue();
		Path outFile = new Path("nb_comparaison2.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		br.write(String.valueOf(counter));
		br.close();
		return 0;
	}

	public static class Map extends Mapper<Text, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordsL = value.toString().split(" ");
			long nbofwordkeep = Math.round(wordsL.length
					- (wordsL.length * 0.8) + 1);
			String[] wordstokeepL = Arrays.copyOfRange(wordsL, 0,
					(int) nbofwordkeep);

			for (String wordtokeep : wordstokeepL) {
				word.set(wordtokeep);
				context.write(word, key);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

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
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, String> linesHP = new HashMap<String, String>();
			reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/Assignment2/input_preprocessing/part-r-00000")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] line = pattern.split("\t");
				linesHP.put(line[0], line[1].replaceAll(",", ""));;
			}

			ArrayList<String> wordsL = new ArrayList<String>();

			for (Text word : values) {
				wordsL.add(word.toString());
			}

			if (wordsL.size() > 1) {
				ArrayList<String> pairs = new ArrayList<String>();
				for (int i = 0; i < wordsL.size(); ++i) {
					for (int j = i + 1; j < wordsL.size(); ++j) {
						String pair = new String(wordsL.get(i) + " "
								+ wordsL.get(j));
						pairs.add(pair);
					}
				}

				for (String pair : pairs) {
					TreeSet<String> wordsof1stlineinpairTS = new TreeSet<String>();
					String wordsof1stlineinpairS = linesHP
							.get(pair.split(" ")[0].toString());
					for (String word : wordsof1stlineinpairS.split(" ")) {
						wordsof1stlineinpairTS.add(word);
					}

					TreeSet<String> otherwords = new TreeSet<String>();
					String words = linesHP
							.get(pair.split(" ")[1].toString());
					for (String word : words.split(" ")) {
						otherwords.add(word);
					}

					context.getCounter(counters.nb_comparaison)
							.increment(1);
					double sim = jaccard(wordsof1stlineinpairTS,
							otherwords);

					if (sim >= 0.8) {
						context.write(new Text("(" + pair.split(" ")[0] + ", "
								+ pair.split(" ")[1] + ")"),
								new Text(String.valueOf(sim)));
					}
				}
			}
		}
	}
}