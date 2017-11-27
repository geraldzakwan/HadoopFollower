import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.*;

public class TwitterLevelTwoFollowers {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context	) throws IOException, InterruptedException {
			StringTokenizer itr  = new StringTokenizer(value.toString());
			if (itr.hasMoreTokens()) {
				String userId = itr.nextToken();
				if (itr.hasMoreTokens()) {
					String followerId = itr.nextToken();
					context.write(new Text(userId), new Text(followerId + " 0"));
					context.write(new Text(followerId), new Text(userId + " 1"));
				}	
			}
		}
	}

	public static class GroupReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			HashSet<String> hash = new HashSet<String>();
			for (Text value : values) {
				String [] input = value.toString().split(" ");
				if (input[1] == "1") {
					hash.add(input[0]);
				} else {
					sb.append(" "+input[0]);
				}
			}
			context.write(key, new Text(sb.toString().trim()));
			for (String val : hash) {
				context.write(new Text(val), new Text(sb.toString().trim()));
			}
		}
	}
	
	public static class FollowingMapper extends Mapper<Object, Text, Text, Text> {
                       private static Text userId = new Text();
                       private static Text followerId = new Text();
                public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
                        StringTokenizer itr  = new StringTokenizer(value.toString());
                        if (itr.hasMoreTokens()) {
                        	userId.set(itr.nextToken());
                        	while (itr.hasMoreTokens()) {
						followerId.set(itr.nextToken());
						context.write(userId, followerId);
                        	}
                        }
                }
        }
	
	public static class User {
		public Text userId;
		public int followerCount;
		
		public User(Text uid, int count) {
			userId = new Text(uid.toString());
			followerCount = count;
		}

		public static Comparator<User> getCompByCount() {
			Comparator<User> comp = new Comparator<User>() {
				@Override
				public int compare(User u1, User u2) {
					return u1.followerCount > u2.followerCount ? -1 : u1.followerCount == u2.followerCount ? 0 : 1;
				}
			};
			return comp;
		}
	}
	
	public static class FollowingReducer extends Reducer<Text,Text,Text,Text> {
		private static ArrayList<User> countList = new ArrayList<User>();
		private static Text count = new Text();
         	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				HashSet<String> values2 = new HashSet<String>();
                for (Text val : values) {
                        values2.add(val.toString().trim());
				}
			countList.add(new User(key, values2.size()));
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Collections.sort(countList, User.getCompByCount());
			int counter = 0;
            		for (User u : countList) {
		                if (counter ++ == 10) {
                			break;
		                }
				count.set(Integer.toString(u.followerCount));
        	        	context.write(u.userId, count);
	        	 }
		}
        }


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Twitter Level Two Followers - 1");
		job.setJarByClass(TwitterLevelTwoFollowers.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setCombinerClass(GroupReducer.class);
		job.setReducerClass(GroupReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
                Job job2 = Job.getInstance(conf2, "Twitter Level Two Followers - 2");
                job2.setJarByClass(TwitterLevelTwoFollowers.class);
		job2.setMapperClass(FollowingMapper.class);
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                // job2.setCombinerClass(FollowingReducer.class);
                job2.setReducerClass(FollowingReducer.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job2, new Path(args[1]));
                FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
	}
}