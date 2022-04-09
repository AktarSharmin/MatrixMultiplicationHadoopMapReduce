import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;

public class Main {

	public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String str = value.toString();
			String[] in = str.split(" ");
			
			Text outKey = new Text();
			Text outValue = new Text();

			if (in[0].equals("M")) {
				for (int i = 0; i < 1; i++) {
					outKey.set(in[1] + " " + i);
					outValue.set("M " + in[2] + " " + in[3]);
					context.write(outKey, outValue);
				}
			} else {
				for (int j = 0; j < 1100; j++) {
					outKey.set(j + " " + in[2]);
					outValue.set("V " + in[1] + " " + in[3]);
					context.write(outKey, outValue);
				}
			}
		}
	}

	public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] value;
			HashMap<Integer, Float> HashM = new HashMap<Integer, Float>();
			HashMap<Integer, Float> HashV = new HashMap<Integer, Float>();

			for (Text val : values) {
				value = val.toString().split(" ");
				if (value[0].equals("M")) {
					HashM.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				} else {
					HashV.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
			}
			
			float result = 0.0f, M, V;
			

			for (int k = 0; k < 1100; k++) {
				M = HashM.containsKey(k) ? HashM.get(k) : 0.0f;
				V = HashV.containsKey(k) ? HashV.get(k) : 0.0f;
				result += M * V;
			}
			if (result != 0.0f) {
				context.write(null, new Text(key.toString() + " " + Float.toString(result)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		

	    
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(Main.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MatrixMapper.class);
		job.setReducerClass(MatrixReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
		
	}
}
