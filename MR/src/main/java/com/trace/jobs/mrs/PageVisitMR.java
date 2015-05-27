package com.trace.jobs.mrs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.trace.jobs.pojo.PageVisitBean;


public class PageVisitMR {

	public static class PageVisitMapper extends Mapper<Object, Text, Text, PageVisitBean>{

		private static final IntWritable one = new IntWritable(1);
		private PageVisitBean pv = new PageVisitBean();
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, PageVisitBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] strContent = value.toString().split(",");
			String activityName = strContent[3];
			String activityduration = strContent[6];
			pv.setActivity_count(one.get());
			pv.setSum_duration_count(Long.valueOf(activityduration));
			context.write(new Text(activityName), pv);
		}
	}
	
	public static class PageVisitReducer extends Reducer<Text, PageVisitBean, Text, PageVisitBean>{

		@Override
		protected void reduce(Text key, Iterable<PageVisitBean> values,
				Reducer<Text, PageVisitBean, Text, PageVisitBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long activity_count = 0;
			long sum_duration_count = 0;
			PageVisitBean pv = new PageVisitBean();
			for (PageVisitBean pvb : values) {
				activity_count += pvb.getActivity_count();
				sum_duration_count += pvb.getSum_duration_count();
			}
			pv.setActivity_count(activity_count);
			pv.setSum_duration_count(sum_duration_count);
			pv.setAvg_duration_count(sum_duration_count / activity_count);
			context.write(key, pv);
		}
	}
	
	public int run(String [] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, PageVisitMR.class.getSimpleName());
		
		job.setJarByClass(PageVisitMR.class);
		
		Path inputDir = new Path("hdfs://192.168.13.17:9000/data/in/10269_activity_20150630.log");
		FileInputFormat.addInputPath(job, inputDir);
		
		job.setMapperClass(PageVisitMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageVisitBean.class);
		
		job.setReducerClass(PageVisitReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageVisitBean.class);
		
		Path outputDir = new Path("hdfs://192.168.13.17:9000/data/activity");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: 请输入输入输出路径 ......");
			System.exit(2);
		}*/

		// 运行任务
		int status = new PageVisitMR().run(args);

		// 退出
		System.exit(status);
	}
}
