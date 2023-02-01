import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.*;

public class IMDBMapreduce {
	public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String str = value.toString();
			String[] result = str.toString().split(";");
			if (result.length == 5) {
				int n = result.length;
				String year = result[n - 2];
				String type = result[n - 4];
				int setone = 0;
				int settwo = 0;
				int setthree = 0;
				for (int i = 2001; i <= 2015; i =i+ 5) {
					if (!year.equals("\\N") && type.equals("movie")) {
						setone = 0;
						settwo = 0;
						setthree = 0;
						int lyear = i + 4;
						if (Integer.parseInt(year) >= i && Integer.parseInt(year) <= lyear) {
							String genreList = result[n - 1];
							String genreArray[] = genreList.split(",");
							List<String> genre = new ArrayList<>(Arrays.asList(genreArray));
							if(!genre.contains("\\N")){
								if (genre.contains("Comedy") && genre.contains("Romance")) {
									setone++;
									}
								if (genre.contains("Action") && genre.contains("Thriller")) {
										settwo++;
									}
								if (genre.contains("Adventure") && genre.contains("Sci-Fi")) {
										setthree++;
									}
									}
							if(setone == 1) {
								String mapkey = "Comedy,Romance" + " " + i + "-" + lyear;
								word.set(mapkey);
								context.write(word, one);
							}
							if(settwo == 1) {
								String mapkey1 = "Action,Thriller" + " " + i + "-" + lyear;
								word.set(mapkey1);
								context.write(word, one);
							}
							if(setthree ==1) {
								String mapkey2 = "Adventure,Sci-Fi" + " " + i + "-" + lyear;
								word.set(mapkey2);
								context.write(word, one);
							}
						}
					}
				}

			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

		/*	String strArr[] = key.toString().split(" ");
			if (strArr.length > 1) {
				String year = strArr[1];
				String genre = strArr[0];

				String newKey = year + "," + genre;
				key.set(newKey);
			}*/
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(IMDBMapreduce.class);
	    job.setMapperClass(TokenizerMapper1.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
