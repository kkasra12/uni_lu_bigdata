import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class HadoopWordPairs_p1b4 extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text pair = new Text();
		private Text lastWord = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			for (String w : tokenizer(value.toString(), "[a-z]{1,25}")) {
				if (lastWord.getLength() > 0) {
					pair.set(lastWord + ":" + w);
					context.write(pair, one);
				}
				lastWord.set(w);
			}
		}

		public ArrayList<String> tokenizer(String line, String regex) {
			String[] raw_words = line.replaceAll("[!?.,]", " ").toLowerCase().split("\\s+");
			Pattern word_validator = Pattern.compile(regex);
			ArrayList<String> words = new ArrayList<String>();
			for (String w : raw_words)
				if (word_validator.matcher(w).matches())
					words.add(w);
			return words;
		}

	}

	public static class Map_sorter extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] wordCount = value.toString().split("\t");
			context.write(new IntWritable(Integer.parseInt(wordCount[1])), new Text(wordCount[0]));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values)
				sum += value.get();

			context.write(key, new IntWritable(sum));
		}
	}

	public static class Reduce_sorter extends Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values)
				context.write(value, key);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String tmp_path = "./output_tmp/";

		// Delete tmp_path if it exists
		File tmpDir = new File(tmp_path);
		if (tmpDir.exists() && tmpDir.isDirectory()) {

			File[] files = tmpDir.listFiles();
			if (files != null) {
				for (File file : files) {
					file.delete();
				}
			}
			tmpDir.delete();
		}

		Job job = Job.getInstance(new Configuration(), "HadoopWordPairs_p1b4");
		job.setJarByClass(HadoopWordPairs_p1b4.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(tmp_path));

		job.waitForCompletion(true);

		Job job_sorter = Job.getInstance(new Configuration(), "HadoopWordPairs_p1b4_sorter");
		job_sorter.setJarByClass(HadoopWordPairs_p1b4.class);

		job_sorter.setOutputKeyClass(IntWritable.class);
		job_sorter.setOutputValueClass(Text.class);

		job_sorter.setMapperClass(Map_sorter.class);
		job_sorter.setReducerClass(Reduce_sorter.class);

		job_sorter.setInputFormatClass(TextInputFormat.class);
		job_sorter.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job_sorter, new Path(tmp_path));
		FileOutputFormat.setOutputPath(job_sorter, new Path(args[1]));

		job_sorter.waitForCompletion(true);

		int n = 100;
		String os = System.getProperty("os.name").toLowerCase();
		if (os.contains("linux")) {
			String tempFile = args[1] + "/tempfile";
			ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c",
					"tail -n " + n + " " + args[1] + "/part-r-00000" + " > " + tempFile);
			processBuilder.start().waitFor();
			ProcessBuilder renameBuilder = new ProcessBuilder("mv", tempFile, args[1] + "/part-r-00000");
			renameBuilder.start().waitFor();
			System.out.println("The " + n + " most frequent words are saved in " + args[1] + "/part-r-00000");
		} else {
			System.err.println("This is a non-Linux machine, please trim the " + n + " last lines of the " + args[1]
					+ "/part-r-00000 file manually");
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordPairs_p1b4(), args);
		System.exit(ret);
	}
}
