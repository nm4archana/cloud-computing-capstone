package Task1Q11;

import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class MapReduceNumOfFlights {

	private static Logger LOGGER = null; 
	
	public MapReduceNumOfFlights(String path)
	{
		LOGGER = Log.setLogHandler(MapReduceNumOfFlights.class,path);
		LOGGER.setLevel(Level.INFO);
	}
	
	
	public static class SortComparator extends WritableComparator {

		protected SortComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			IntWritable k1 = (IntWritable) o1;
			IntWritable k2 = (IntWritable) o2;
			int cmp = k1.compareTo(k2);
			return -1 * cmp;
		}

		public static class FirstMapper extends Mapper<Object, Text, Text, IntWritable> {

			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				@SuppressWarnings("resource")
				Scanner s = new Scanner(value.toString()).useDelimiter(",");
				String src = s.next();
				String dst = s.next();

				context.write(new Text(src), new IntWritable(1));
				context.write(new Text(dst), new IntWritable(1));
				s.close();
			}
		}

		public static class FirstReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
			public void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {
				int totalFlights = 0;

				for (IntWritable val : values) {
					totalFlights = totalFlights + Integer.parseInt(val.toString());
				}

				context.write(new IntWritable(totalFlights), new Text(key + ":" + totalFlights));
			}
		}

		public static class SecondMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

			public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

				context.write(key, value);

			}
		}

		public static class ThirdMapper extends Mapper<IntWritable, Text, Text, Text> {

			public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

				context.write(value,new Text(""));

			}
		}

		public static void main(String[] args) throws Exception 
		{		
			new MapReduceNumOfFlights(args[2]);
			Job job1 = Job.getInstance();
			job1.setJobName("NumberOfFlights");
			job1.setJarByClass(MapReduceNumOfFlights.class);

			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(Text.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);

			job1.setMapperClass(FirstMapper.class);
			job1.setReducerClass(FirstReducer.class);

			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
			/*FileInputFormat.setInputPaths(job1, new Path(
				"/Users/archana/Documents/Projects/Capstone/input_data/task1/Q1.1/data_cleaned/data.txt"));*/
			
			FileInputFormat.setInputPaths(job1, new Path(args[0]+"/InputDataFile_Task1Q11.txt"));	
			
	        /*FileOutputFormat.setOutputPath(job1, new Path("/Users/archana/Documents/Projects/Capstone/output" + "/f0"));*/

			FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f0"));
			
			job1.waitForCompletion(true);

			Job job2 = Job.getInstance();
			job2.setJobName("NumberOfFlights");
			job2.setJarByClass(MapReduceNumOfFlights.class);

			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);

			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setMapperClass(SecondMapper.class);

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);

			job2.setSortComparatorClass(SortComparator.class);
       
			
			//FileInputFormat.setInputPaths(job2, new Path("/Users/archana/Documents/Projects/Capstone/output" + "/f0"));
			//FileOutputFormat.setOutputPath(job2, new Path("/Users/archana/Documents/Projects/Capstone/output" + "/f1"));
			
			FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f0"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f1"));

			job2.waitForCompletion(true);

			Job job3 = Job.getInstance();
			job3.setJobName("NumberOfFlights");
			job3.setJarByClass(MapReduceNumOfFlights.class);

			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);

			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);

			job3.setMapperClass(ThirdMapper.class);
			job3.setNumReduceTasks(0);
			
			job3.setInputFormatClass(SequenceFileInputFormat.class);
			job3.setOutputFormatClass(TextOutputFormat.class);

			//FileInputFormat.setInputPaths(job3, new Path("/Users/archana/Documents/Projects/Capstone/output" + "/f1"));
			//FileOutputFormat.setOutputPath(job3, new Path("/Users/archana/Documents/Projects/Capstone/output" + "/f2"));

			FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f1"));
			FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/f2"));

			job3.waitForCompletion(true);

		}
	}
}
