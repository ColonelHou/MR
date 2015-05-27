package com.trace.jobs.mrs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.trace.jobs.pojo.VersionDistriMapBean;
import com.trace.jobs.pojo.VersionDistriRedBean;

public class VersionDistriMR {

	public static class VersionDistriMapper extends
			Mapper<Object, Text, Text, VersionDistriMapBean> {

		private static final IntWritable one = new IntWritable(1);
		private VersionDistriMapBean ver = new VersionDistriMapBean();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, VersionDistriMapBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] strContent = value.toString().split(",");
			System.out.println(Arrays.toString(strContent));
			try {
				String application_ver = strContent[2];
				String duration = strContent[7];
				String terminal_id = strContent[8];
				ver.setApplication_ver(application_ver);
				ver.setDuration(Long.parseLong(duration));
				ver.setTerminal_id(terminal_id);
				context.write(new Text(application_ver), ver);
			} catch (Exception e) {
				System.out.println("异常了: " + e.getMessage());
				return;
			}
		}
	}

	public static class VersionDistriReducer extends
			Reducer<Text, VersionDistriMapBean, Text, VersionDistriRedBean> {

		@Override
		protected void reduce(
				Text key,
				Iterable<VersionDistriMapBean> values,
				Reducer<Text, VersionDistriMapBean, Text, VersionDistriRedBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//注意需要添加UnKnow次数
			try {
				VersionDistriRedBean rdb = new VersionDistriRedBean();
				Map<String, Integer> terminalMap = new HashMap<String, Integer>();
				long day_user_count = 0;
				long day_session_count = 0;
				long day_duration_count = 0;
				for (VersionDistriMapBean verBean : values) {
					day_session_count += 1;
					day_duration_count += verBean.getDuration();
					if (terminalMap.get(verBean.getTerminal_id()) == null) {
						terminalMap.put(verBean.getTerminal_id(), 1);
						day_user_count += 1;
					}
				}
				rdb.setApplication_ver(key.toString());
				rdb.setDay_session_count(day_session_count);
				rdb.setDay_user_count(day_user_count);
				rdb.setDuration(day_duration_count);
				context.write(key, rdb);
			} catch (Exception e) {
				System.out.println("异常了: " + e.getMessage());
				return;
			}
		}
	}

	public int run(String[] args, Configuration conf) throws IOException,
			ClassNotFoundException, InterruptedException {

		//conf.setIfUnset("mapreduce.job.ubertask.enable", "true");
		System.out.println(conf.get("mapreduce.job.ubertask.enable"));
		// 创建任务
		Job job = new Job(conf, VersionDistriMR.class.getSimpleName());
		// 设置任务
		job.setJarByClass(VersionDistriMR.class);
		// A.input
		Path inputDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputDir);

		// B.Map
		job.setMapperClass(VersionDistriMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VersionDistriMapBean.class);

		// C.Reduce
		job.setReducerClass(VersionDistriReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VersionDistriRedBean.class);

		// D.output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);

		// 提交任务
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	/**
	 * yarn jar MR.jar com.trace.jobs.mrs.VersionDistriMR
	 * hdfs://192.168.13.17:9000/data/in/session/10278_session_20150708.log
	 * hdfs://192.168.13.17:9000/data/out/session/
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// 获取配置
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: 请输入输入输出路径 ......");
			System.exit(2);
		}

		// 运行任务
		int status = new VersionDistriMR().run(args, conf);

		// 退出
		System.exit(status);
	}

}
