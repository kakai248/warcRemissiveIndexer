import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;

import edu.cmu.lemurproject.*;
import java.net.URL;

public class SiteWordCount {

	public static class Pair implements Writable {
		private Text key;
		private IntWritable value;

		public Pair() {
			this.key = new Text();
			this.value = new IntWritable();
		}

		public Pair(Text key, IntWritable value) {
			this.key = key;
			this.value = value;
		}

		public Text getKey() {
			return key;
		}

		public IntWritable getValue() {
			return value;
		}

		public String toString() {
			return key + " - " + value;
		}

		public void write(DataOutput out) throws IOException {
			key.write(out);
			value.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			key.readFields(in);
			value.readFields(in);
		}
	}

	public static class MyMap extends Mapper<LongWritable, WritableWarcRecord, Text, Pair> {
		private Text urlText = new Text();
		private Text wordText = new Text();
		private IntWritable intWritable = new IntWritable();

		private boolean validWord(String word) {
			return !word.isEmpty() && (word.charAt(0) >= 65 && word.charAt(0) <= 90 
					|| word.charAt(0) >= 97	&& word.charAt(0) <= 122);
		}

		public void map(LongWritable key, WritableWarcRecord value, Context cont )
			throws IOException, InterruptedException {

			Map<String, Integer> map = new HashMap<String, Integer>();

			WarcRecord val = value.getRecord();
			String url = val.getHeaderMetadataItem("WARC-Target-URI");

			String[] lines = val.getContentUTF8().split("\n");
			for(String line : lines) {
				String[] words = line.split(" ");
				for(String word : words) {
					if(!validWord(word)) continue;

					int count = map.get(word) == null ? 1 : map.get(word)+1;
					map.put(word, count);
				}
			}

			try {
				urlText.set(url);
				for(String word : map.keySet()) {
					wordText.set(word);
					intWritable.set(map.get(word));

					cont.write(wordText, new Pair(urlText, intWritable));
				}
			} catch ( Exception e ) {}
		}
	}

	public static class MyReduce extends Reducer<Text, Pair, Text, Pair> {

		public void reduce(Text key, Iterable<Pair> values, Context cont)
			throws IOException, InterruptedException {
			
			for(Pair val : values) {
				Pair result = new Pair(val.getKey(), val.getValue());
				cont.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Job conf = Job.getInstance( new Configuration(), "sitewordcount" );
		conf.setJarByClass(SiteWordCount.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Pair.class);

		conf.setMapperClass(MyMap.class);
		conf.setCombinerClass(MyReduce.class);
		conf.setReducerClass(MyReduce.class);

		conf.setInputFormatClass(WarcFileInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.waitForCompletion(true); // submit and wait
	}
}
