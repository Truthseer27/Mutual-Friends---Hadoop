
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.StringUtils;


public class Assignment1_1 extends Configured {

	public static class Ass1_1Mapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text friendsList = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String input_line = value.toString();
			
			String[] input_split = input_line.split("\t");
			
			String current_User = input_split[0];
			Long current_UserL = Long.parseLong(current_User);
			
			if (input_split.length == 2) {
				
				String listOfFriends = input_split[1];
				
				String[] friendsListArray = input_split[1].split(",");
				
				for(String friend:friendsListArray){
				
					friendsList.set(listOfFriends);					
					
					ArrayList<Integer> keyList = new ArrayList<Integer>();
					
					if (current_UserL.compareTo(Long.parseLong(friend)) < 0) {

					
						keyList.clear();
						keyList.add(Integer.parseInt(current_UserL.toString()));
						keyList.add(Integer.parseInt(friend));
						
						context.write(new Text(StringUtils.join(",", keyList)), friendsList);
					

					} else {
						
						keyList.clear();
						
						keyList.add(Integer.parseInt(friend));
						keyList.add(Integer.parseInt(current_UserL.toString()));
						
						
						context.write(new Text(StringUtils.join(",", keyList)), friendsList);
						
						
					}
				
			}

			}
		}
	}

	public static class Ass1_1Reducer extends Reducer<Text, Text, Text, Text> {
		

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] groupedFriends = new String[2];
			int current = 0;
			for (Text v : values) {
				groupedFriends[current++] = v.toString();
			}

			if (null != groupedFriends[0]) {
				groupedFriends[0] = groupedFriends[0].replaceAll("[^0-9,]", "");

			}
			if (null != groupedFriends[1]) {
				groupedFriends[1] = groupedFriends[1].replaceAll("[^0-9,]", "");
			}

			String[] list1 = groupedFriends[0].split(",");
			String[] list2 = groupedFriends[1].split(",");
			
			
			//StringBuilder output = new StringBuilder();
			ArrayList<Integer> alist1=new ArrayList<Integer>();
			ArrayList<Integer> finalList = new ArrayList<Integer>();
			
			if(null != groupedFriends[0]){
				for (String str:list1){
					alist1.add(Integer.parseInt(str));
				}
			}
			
			if(null != groupedFriends[1]){
				for(String str:list2){
					if(alist1.contains(Integer.parseInt(str))){
						finalList.add(Integer.parseInt(str));
					}
				}
			}
		
//			output.setLength(output.length() - 1);
//			context.write(new Text(key.toString()), new Text(output.toString()));
			
			
			context.write(new Text(key.toString()), new Text(StringUtils.join(",", finalList)));

		}
	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		
		conf.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"));
		
		Job job = Job.getInstance(conf, "Assignment1_1");
		
		job.setJarByClass(Assignment1_1.class);

		job.setMapperClass(Ass1_1Mapper.class);
		job.setReducerClass(Ass1_1Reducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	
}