import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Assignment1_4 
{
    static int i=0;    
    public static class Ass1_4Mapper1 extends Mapper<LongWritable, Text, LongWritable, Text> 
    {
           LongWritable current_user =new LongWritable();
           // Text friends=new Text();

        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
             String line[] = value.toString().split("\t");
                current_user.set(Long.parseLong(line[0]));
                //Text data=new Text();
                //data.set(current_user.toString());
              
                if (line.length != 1)
                {
                    String friendsList = line[1];
                    String mapper1Output=("U:" + friendsList.toString());
                    context.write(current_user, new Text(mapper1Output));
                }
            }
        }
    
    
    public static class Ass1_4Mapper2 extends Mapper<LongWritable, Text, LongWritable, Text> 
    {
        private LongWritable mapper2Key = new LongWritable();
        private Text mapper2value = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
             String userInfo[] = value.toString().split(",");
                if(userInfo.length == 10){
                    
                mapper2Key.set(Long.parseLong(userInfo[0]));
                String data=userInfo[1]+","+userInfo[3]+","+userInfo[4]+","+userInfo[5];
                mapper2value.set("R:" + data);
                        //System.out.println(mapper2value);
                context.write(mapper2Key, mapper2value);
            }
        }

    }
    
    public static class Ass1_4Reducer1 extends Reducer<LongWritable, Text, Text, Text> 
    {
    	private ArrayList<Text> mapper1_List = new ArrayList<Text>();
		private ArrayList<Text> mapper2_List = new ArrayList<Text>();
        HashMap<String, String> hashMap=new HashMap<>();
        int Month = 0, Day = 0, Year = 0;
         public void setup(Context context) throws IOException {
                Configuration conf = context.getConfiguration();
                          hashMap = new HashMap<String,String>();
                 String userdataPath = conf.get("businessdata");
                 
                Path path=new Path("hdfs://localhost:9000"+userdataPath);//Location of file in HDFS
                FileSystem fileSystem = FileSystem.get(conf);
                BufferedReader bufferedreader=new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                String inputLine;
                inputLine=bufferedreader.readLine();
                while (inputLine != null){
                    String[] arr=inputLine.split(",");
                    if(arr.length == 10){
                    hashMap.put(arr[0].trim(), arr[1]+":"+arr[3]+":"+arr[9]); 
                    }
                    inputLine=bufferedreader.readLine();
                }  
                
                Calendar cal = Calendar.getInstance();
    			Month = cal.get(Calendar.MONTH);
    			Year = cal.get(Calendar.YEAR);
    			Day = cal.get(Calendar.DAY_OF_MONTH);
                
            }
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // Clear our lists
            mapper1_List.clear();
            mapper2_List.clear();
            
            for (Text value : values)
            {
                if (value.toString().charAt(0) == 'U')
                {
                    mapper1_List.add(new Text(value.toString().substring(2)));
                }         
                else if (value.toString().charAt(0) == 'R') 
                {
                    mapper2_List.add(new Text(value.toString().substring(2)));
                }
            }//End_For
            Text reducer1OutputVal=new Text();
            int age=0;
           
            int max = 0;
            String[] details = null;
            //Actual Joining of two files
            if(!mapper1_List.isEmpty() && !mapper2_List.isEmpty())
            {
                for(Text A : mapper1_List)
                {
                    String friend[]=A.toString().split(",");
                    
                            for(int i=0;i<friend.length;i++)
                            {
                                if(hashMap.containsKey(friend[i]))
                                {
                                    String[] ageSplit=hashMap.get(friend[i]).split(":");
                                     
                                       String[] birthdateArray=ageSplit[2].toString().split("/");
                                       int result = Year - Integer.parseInt(birthdateArray[2]);

                                        if (Integer.parseInt(birthdateArray[0]) > Month) {
                                            result--;
                                        }
                                        else if (Integer.parseInt(birthdateArray[0]) == Month) {
                                            //int nowDay = now.getDate();

                                            if (Integer.parseInt(birthdateArray[1]) > Day) {
                                                result--;
                                            }
                                        }
                                        age = result;
                                        //System.out.println(age);
                                        if (age > max){
                                        	max = age;
                                        }
                                        //count++;
                                }
                            }
                         
                            
                            String string = "";
                            
                            for(Text B : mapper2_List)
                            {
                                 details = B.toString().split(",");
                                 string = B.toString() + "," + new Text(Integer.toString(max));
                            }
                            
                            reducer1OutputVal.set(string);
                    }
                
            }
            context.write(new Text(key.toString()),reducer1OutputVal);    
            }
    
    }
    
    
    public static class Ass1_4Mapper3 extends Mapper<LongWritable, Text, IntWritable, Text> {
		
    	private Long mapper3OutputKey = new Long(0L);
		

		public Long getOutkey() {
			return mapper3OutputKey;
		}

		public void setOutkey(Long outkey) {
			this.mapper3OutputKey = outkey;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] inputValue = value.toString().split("\t");
			Long l = Long.parseLong(inputValue[0]);
			mapper3OutputKey = l;
			ArrayList<String> arrayList = new ArrayList<String>();
			if (inputValue.length == 2) {
				arrayList.clear();
				String line[] = inputValue[1].split(",");
				arrayList.add(inputValue[0]);
				arrayList.add(line[4]);
				context.write(new IntWritable(0-Integer.parseInt(line[4])),
						new Text(inputValue[1].toString()));
			}
		}

	}
    
     
    public static class Ass1_4Reducer2 extends Reducer<IntWritable, Text, Text, Text> 
    {
        
        int i=0;
        //TreeMap<String,String> hash=new TreeMap<String, String>();        
        ArrayList<String> listofall=new ArrayList<String>();
      //  HashMap<String, String> Map = new HashMap<String, String>();     
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            
            for(Text value:values)
            {
            	
                if(listofall.size()<=10)
                {
                	listofall.add(value.toString());
                    
                }
            }
            
        }
        public void cleanup(Context context) throws IOException,InterruptedException{
        	for(String value:listofall){
        		context.write(new Text(""), new Text(value));
        	}
        }
         
        
    }
//
//    
//    
    
    
    //Driver code
    public static void main(String[] args) throws Exception 
    {

        Path outputDirIntermediate1 = new Path(args[3] + "_int1");
        Path outputDirIntermediate2 = new Path(args[3] + "_int2");
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        
        conf.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/core-site.xml"));
      	conf.addResource(new Path("/Users/Ishan/Downloads/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"));
      		
      		
        conf.set("businessdata",otherArgs[0]);
        
        Job job = Job.getInstance(conf, "join1");
        job.setJarByClass(Assignment1_4.class);
        job.setReducerClass(Ass1_4Reducer1.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, Ass1_4Mapper1.class );
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, Ass1_4Mapper2.class );

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
    
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job,outputDirIntermediate1);
        
      //  System.exit(
        int code = job.waitForCompletion(true) ? 0 : 1;
        //System.out.println(code);
     
        Job job1 = Job.getInstance(conf, "join2");
        job1.setJarByClass(Assignment1_4.class);

        FileInputFormat.addInputPath(job1, outputDirIntermediate1);
//        
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
      
         job1.setMapperClass(Ass1_4Mapper3.class);
         
         job1.setReducerClass(Ass1_4Reducer2.class);
         job1.setOutputKeyClass(Text.class);
         job1.setOutputValueClass(Text.class);

         FileOutputFormat.setOutputPath(job1,outputDirIntermediate2);
//        
//                
//        // Execute job and grab exit code
         System.exit(job1.waitForCompletion(true) ? 0 : 1);
//        
  }



}