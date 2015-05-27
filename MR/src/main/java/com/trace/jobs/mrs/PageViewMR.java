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
import org.apache.hadoop.util.GenericOptionsParser;

import com.trace.jobs.pojo.PageViewBean;

public class PageViewMR {

	public static class PageViewMapper extends
			Mapper<Object, Text, Text, PageViewBean> {

		private final static IntWritable one = new IntWritable(1);
		private PageViewBean pv = new PageViewBean();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, PageViewBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 北京,30,50
			String[] val = value.toString().split(",");
			Text activityName = new Text(val[0]);
			long activity_count = Long.valueOf(val[1]);
			pv.setActivity_count(activity_count);
			long sum_duration_count = Long.valueOf(val[2]);
			pv.setSum_duration_count(sum_duration_count);
			pv.setActivityNum(one.get());
			context.write(activityName, pv);
		}

	}

	public static class PageViewRed extends
			Reducer<Text, PageViewBean, Text, PageViewBean> {

		@Override
		protected void reduce(Text key, Iterable<PageViewBean> values,
				Reducer<Text, PageViewBean, Text, PageViewBean>.Context context)
				throws IOException, InterruptedException {
			PageViewBean pvb = new PageViewBean();
			long activityCount = 0;
			long durationCount = 0;
			int num = 0;
			long avg = 0;
			for (PageViewBean pv : values) {
				activityCount += pv.getActivity_count();
				durationCount += pv.getSum_duration_count();
				num += pv.getActivityNum();
			}
			pvb.setActivity_count(activityCount);
			pvb.setSum_duration_count(durationCount);
			pvb.setActivityNum(num);
			avg = pvb.getActivity_count() / pvb.getActivityNum();
			pvb.setAvg_duration_count(avg);
			context.write(key, pvb);
		}

	}

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		// 获取配置
		Configuration conf = new Configuration();

		// 创建任务
		Job job = new Job(conf, PageViewMR.class.getSimpleName());

		// 设置任务
		job.setJarByClass(PageViewMR.class);
		// A.input
		Path inputDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputDir);

		// B.Map
		job.setMapperClass(PageViewMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewBean.class);

		// C.Reduce
		job.setReducerClass(PageViewRed.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageViewBean.class);

		// D.output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);

		// 提交任务
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// 解析输入参数
		String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: 请输入输入输出路径 ......");
			System.exit(2);
		}

		// 运行任务
		int status = new PageViewMR().run(args);

		// 退出
		System.exit(status);
	}
}
