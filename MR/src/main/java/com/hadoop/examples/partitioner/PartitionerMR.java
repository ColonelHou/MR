package com.hadoop.examples.partitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * [hadoop@hadoopMaster ]$cat a.log
    shoes,20
    hat,10
    stockings,30
    clothes,40
    [hadoop@hadoopMaster ]$cat b.log
    shoes,15
    hat,1
    stockings,90
    clothes,80
 * 
 * [hadoop@hadoopMaster ]$hdfs dfs -ls -R /data/partition
 -rw-r--r--   2 hadoop supergroup          0 2015-07-06 16:38 /data/partition/_SUCCESS
 -rw-r--r--   2 hadoop supergroup          9 2015-07-06 16:38 /data/partition/part-r-00000
 -rw-r--r--   2 hadoop supergroup          7 2015-07-06 16:38 /data/partition/part-r-00001
 -rw-r--r--   2 hadoop supergroup         14 2015-07-06 16:38 /data/partition/part-r-00002
 -rw-r--r--   2 hadoop supergroup         12 2015-07-06 16:38 /data/partition/part-r-00003
 [hadoop@hadoopMaster ]$hdfs dfs -cat /data/partition/part-r-00000
 shoes   35
 [hadoop@hadoopMaster ]$hdfs dfs -cat /data/partition/part-r-00001
 hat     11
 [hadoop@hadoopMaster ]$hdfs dfs -cat /data/partition/part-r-00002
 stockings       120
 [hadoop@hadoopMaster ]$hdfs dfs -cat /data/partition/part-r-00003
 clothes 120
  如果分的R多的话，数据对应少的话就只会生成空的part文件

 * @author John
 *
 */
public class PartitionerMR {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] s = value.toString().split(",");
			context.write(new Text(s[0]),
					new IntWritable(Integer.parseInt(s[1])));
		}
	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class MyPartitioner extends Partitioner<Object, Object> {

		@Override
		public int getPartition(Object key, Object value, int numPartitions) {
			if (key.toString().equals("shoes")) {
				return 0;
			}
			if (key.toString().equals("hat")) {
				return 1;
			}
			if (key.toString().equals("stockings")) {
				return 2;
			}
			return 3;
		}

	}

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, PartitionerMR.class.getSimpleName());
		job.setJarByClass(PartitionerMR.class);

		Path inputDir = new Path("hdfs://192.168.13.17:9000/data/in/partition");
		FileInputFormat.addInputPath(job, inputDir);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(6);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path outputDir = new Path("hdfs://192.168.13.17:9000/data/partition");
		FileOutputFormat.setOutputPath(job, outputDir);
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		// 运行任务
		int status = new PartitionerMR().run(args);

		// 退出
		System.exit(status);
	}

}
