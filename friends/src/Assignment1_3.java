
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Assignment1_3 extends Configured {
	

	public static class Ass1_3Mapper extends Mapper<LongWritable, Text, Text, Text> {
		static HashMap<String, String> userDetails;
		
		String friendDetail;
		
		public void setup(Context context) throws IOException{
			Configuration config = context.getConfiguration();

			userDetails = new HashMap<String, String>();
			String userdataPath = config.get("userDataPath");
			//System.out.println(userdataPath);
			// Location of file in HDFS
			Path path = new Path("hdfs://localhost:9000" +userdataPath);
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
			String userDataInput;
			userDataInput = bufferedReader.readLine();
			while (userDataInput != null) {
				String[] tempArray = userDataInput.split(",");
				if (tempArray.length == 10) {
					String relevantData = tempArray[1] + ":" + tempArray[9];
					userDetails.put(tempArray[0].trim(), relevantData);
				}
				userDataInput = bufferedReader.readLine();
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println("c"+key);
			
			
			
			String inputValue = value.toString();
			String[] inputValueSplit = inputValue.split("\t");
			
			
			String[] friends = inputValueSplit[1].split(",");
			//System.out.println(userDataInput);
			if (null != userDetails && !userDetails.isEmpty()) {
				for (String friend : friends) {
					if (userDetails.containsKey(friend)) {
						friendDetail = userDetails.get(friend);
						userDetails.remove(friend);
						context.write(new Text(inputValueSplit[0].toString()), new Text(friendDetail));
					}
				}
			}
			
			//System.out.println("check");
			
		}
	}

	public static class Ass1_3Reducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//System.out.println("reduce");
			
			
			ArrayList<String> arrayList= new ArrayList<String>();
		
			//arrayList.add("[");
			for (Text value : values) {
				
				arrayList.add(value.toString());
			}
			
		
			//arrayList.remove(1);
			//arrayList.add("]");
			//counter.set(s);
			context.write(key, new Text("[" + StringUtils.join(",", arrayList) + "]"));
		}
	}
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		
		conf.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"));
				
	
		conf.set("userA", args[0]);
		conf.set("userB", args[1]);	
		
		
		Job job = Job.getInstance(conf, "Assignment1_2");
		
		job.setJarByClass(Assignment1_3.class);
		
		job.setMapperClass(Assignment1_2.Ass1_2Mapper.class);
		job.setReducerClass(Assignment1_2.Ass1_2Reducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		int code = job.waitForCompletion(true) ? 0 : 1;
		System.out.println(code);
		Configuration conf1 = new Configuration();
		
		
		conf1.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/core-site.xml"));
		conf1.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"));
		conf1.set("userDataPath", args[4]);
	
		Job job2 = Job.getInstance(conf1, "Assignment1_3");
		job2.setJarByClass(Assignment1_3.class);
		

		job2.setMapperClass(Ass1_3Mapper.class);
		job2.setReducerClass(Ass1_3Reducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[3]));
		FileOutputFormat.setOutputPath(job2, new Path(args[5]));

		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

	
}