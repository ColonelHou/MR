package com.trace.jobs.mrs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.trace.jobs.pojo.ModelBean;

/**
 * 
 * 模板MR
 * 
 * @author
 *
 */
public class TemplateMR {

	public static class TemplateMapper extends
			Mapper<Object, Text, Text, ModelBean> {

		private static final IntWritable one = new IntWritable(1);
		private ModelBean pv = new ModelBean();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, ModelBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}
	}

	public static class TemplateReducer extends
			Reducer<Text, ModelBean, Text, ModelBean> {

		@Override
		protected void reduce(
				Text key,
				Iterable<ModelBean> values,
				Reducer<Text, ModelBean, Text, ModelBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}
	}

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		// 获取配置
		Configuration conf = new Configuration();

		// 创建任务
		Job job = new Job(conf, TemplateMR.class.getSimpleName());

		// 设置任务
		job.setJarByClass(TemplateMR.class);
		// A.input
		Path inputDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputDir);

		// B.Map
		job.setMapperClass(TemplateMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ModelBean.class);

		// C.Reduce
		job.setReducerClass(TemplateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ModelBean.class);

		// D.output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);

		// 提交任务
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		/*
		 * String[] otherArgs = new
		 * GenericOptionsParser(args).getRemainingArgs(); if (otherArgs.length
		 * != 2) { System.err.println("Usage: 请输入输入输出路径 ......");
		 * System.exit(2); }
		 */

		// 运行任务
		int status = new TemplateMR().run(args);

		// 退出
		System.exit(status);
	}

}
