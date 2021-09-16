package friendrecommendationsystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;



public class FRS extends Configured implements Tool {
	// Define writable for value
	public static class isFrd_Writable implements Writable {
        private Text user_id;
        private IntWritable friend_status;

        public isFrd_Writable() {
            this.user_id = new Text();
            this.friend_status = new IntWritable();
        }
        
        public isFrd_Writable(Text new_user_id, IntWritable friend_status) {
            this.user_id = new_user_id;
            this.friend_status = friend_status;
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            this.user_id.readFields(in);
            this.friend_status.readFields(in);
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            user_id.write(out);
            friend_status.write(out);
        }
        
        // function to get user id from value
        public String readUserID() {
            return user_id.toString();
        }
        
        // function to get the friend status from value
        public int get_friend_status() {
            return friend_status.get();
        }

    }
	
	
	// Mapper class
    // map data from string and string pair into string and defined writable pair
	public static class Map extends Mapper<Text, Text, Text, isFrd_Writable> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	// if friend list get friends inside
        	if (value.getLength() != 0) {
                String[] input_data = value.toString().split(",");
                
                // mark all user's friends as friend status "0" with user, since they are friend already, need not to be recommended
                for (int i = 0; i < input_data.length; i++) {
                    context.write(key, new isFrd_Writable(new Text(input_data[i]), new IntWritable(0)));

                    // mark the friend status between each user's friends pairs as 1, since they now get one mutual friend, need to be recommended
                    for (int j = i+1; j < input_data.length; j++) {
                    	context.write(new Text(input_data[i]), new isFrd_Writable(new Text(input_data[j]), new IntWritable(1)));
                        context.write(new Text(input_data[j]), new isFrd_Writable(new Text(input_data[i]), new IntWritable(1)));
                    }
                }
            }
        }
	}
	
	// Reducer class
    // reduce data from mapper to string and string pair
	public static class Reduce extends Reducer<Text, isFrd_Writable, Text, Text> {
		@Override
	    public void reduce(Text key, Iterable<isFrd_Writable> values, Context context) throws IOException, InterruptedException {
			// to loop through friends of a user(key)
			Iterator<isFrd_Writable> read_users = values.iterator();
			// count the mutual friends of a user in a hashmap
			HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
	        while (read_users.hasNext()) {
                isFrd_Writable user = read_users.next();
                String user_id = user.readUserID();
                
                // if this friend is already a friend of user, assign this friend "-1" as int value
                if (user.get_friend_status() == 0) {
                    hashmap.put(user_id, -1);
                } else {
                	// if this friend is recommended already
                    if (hashmap.containsKey(user_id)) {
                    	// only add user need to be recommended
                    	if (hashmap.get(user_id) != -1) {
                    		// one more mutual friend, add the count value by 1
                    		hashmap.put(user_id, hashmap.get(user_id) + 1);
                        }
                    } else {
                    	// if this friend hasn't been recommended
                        hashmap.put(user_id, 1);
                    }
                }
            }
	        
	        // filter and sort the result in hashmap in a priority queue
	        PriorityQueue<Entry<String, Integer>> ten_most_mutual = new PriorityQueue <Entry<String, Integer>>(10, new Comparator<Entry<String, Integer>>() {
	        	@Override
	            public int compare(Entry<String, Integer> user1, Entry<String, Integer> user2) {
	        		return user2.getValue() - user1.getValue();
	            }
	        });
	        
	        // exclude all friends need not be recommended to the user
	        for (Entry<String, Integer> pairs: hashmap.entrySet()) {
	        	if (!pairs.getValue().equals(-1)) {
	        		ten_most_mutual.add(pairs);
	            }
            }
	        
	        // create a variable to store the top 10 friend recommendation result
	        StringBuffer result = new StringBuffer();
	        int number = 0;
	        int recommendation_number = ten_most_mutual.size();
	         
	        while (ten_most_mutual.isEmpty() == false) {
	        	// get the result from queue and put the "key" value into "result" StringBuffer
	        	result.append(ten_most_mutual.poll().getKey());
	        	
	        	// only output the top 10 recommendations
	        	if (number >= 9 || number >= recommendation_number - 1) {
	            	break;
	            }

	            number++;
	            result.append(",");
	         }
	         
	        context.write(key, new Text(result.toString()));
		} 
	}
	
	// main class
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
        int response = ToolRunner.run(new Configuration(), new FRS(), args);
        System.exit(response);
    }
	
	@Override
    public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "FRS");
        job.setJarByClass(FRS.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(isFrd_Writable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }
	        
}






