package fetch;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import data_structure.url_data;
import org.apache.hadoop.util.GenericOptionsParser;

public class fetcher {

	public void fetch(int d) throws IOException {

		System.out.println("updating db任务执行！！！！");
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath = "/user/hadoop";
		String[] ars1 = new String[] { currentPath + "/crawl/db",
				currentPath + "/crawl/fetch_list", currentPath + "/crawl/tmp" };
		String[] otherArgs1 = new GenericOptionsParser(conf, ars1)
				.getRemainingArgs();
		Path db = new Path(otherArgs1[0]);
		Path fetchlist = new Path(otherArgs1[1]);
		Path tmp = new Path(otherArgs1[2]);
		Job job = new Job(conf, "updating db");
		job.setJarByClass(fetcher.class);
		job.setMapperClass(updatingMap.class);
		job.setCombinerClass(updatingReduce.class);
		job.setReducerClass(updatingReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		if (!FileSystem.get(conf).exists(fetchlist))
			FileSystem.get(conf).mkdirs(fetchlist);
		if (!FileSystem.get(conf).exists(db))
			FileSystem.get(conf).mkdirs(db);
		FileInputFormat.addInputPath(job, fetchlist);
		FileInputFormat.addInputPath(job, db);
		if (FileSystem.get(conf).exists(tmp))
			FileSystem.get(conf).delete(tmp, true);
		FileOutputFormat.setOutputPath(job, tmp);
		try {
			job.waitForCompletion(true);
			if (FileSystem.get(conf).exists(db))
				FileSystem.get(conf).delete(db, true);
			FileSystem.get(conf).rename(tmp, db);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("updating fetchlist任务执行！！！！");
		int depth = d;
		for (int i = 0; i < depth; i++) {
			// conf = new Configuration();
			// conf.set("mapred.job.tracker", "192.168.1.2:9001");
			String[] ars2 = new String[] { currentPath + "/crawl/fetch_list",
					currentPath + "/crawl/tmp" };
			String[] otherArgs2 = new GenericOptionsParser(conf, ars2)
					.getRemainingArgs();
			fetchlist = new Path(otherArgs2[0]);
			tmp = new Path(otherArgs2[1]);
			job = new Job(conf, "updating fetchlist");
			job.setJarByClass(fetcher.class);
			job.setMapperClass(fetchingMap.class);
			job.setCombinerClass(fetchingReduce.class);
			job.setReducerClass(fetchingReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(url_data.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job, fetchlist);
			if (FileSystem.get(conf).exists(tmp))
				FileSystem.get(conf).delete(tmp, true);
			FileOutputFormat.setOutputPath(job, tmp);
			try {
				job.waitForCompletion(true);
				if (FileSystem.get(conf).exists(fetchlist))
					FileSystem.get(conf).delete(fetchlist, true);
				FileSystem.get(conf).rename(tmp, fetchlist);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("crawling 任务执行！！！！");
		// conf = new Configuration();
		// conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String[] ars3 = new String[] {
				currentPath + "/crawl/fetch_list",
				currentPath + "/stored_web/"
						+ new Date(System.currentTimeMillis()) };
		String[] otherArgs3 = new GenericOptionsParser(conf, ars3)
				.getRemainingArgs();
		fetchlist = new Path(otherArgs3[0]);
		Path stored = new Path(otherArgs3[1]);
		job = new Job(conf, "crawling");
		job.setJarByClass(fetcher.class);
		job.setMapperClass(crawlingMap.class);
		job.setCombinerClass(crawlingReduce.class);
		job.setReducerClass(crawlingReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, fetchlist);
		FileOutputFormat.setOutputPath(job, stored);
		try {
			job.waitForCompletion(true);
			if (FileSystem.get(conf).exists(fetchlist))
				FileSystem.get(conf).delete(fetchlist, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {

		System.out.println("updating db任务执行！！！！");
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath = "/user/hadoop";
		String[] ars1 = new String[] { currentPath + "/crawl/db",
				currentPath + "/crawl/fetch_list", currentPath + "/crawl/tmp" };
		String[] otherArgs1 = new GenericOptionsParser(conf, ars1)
				.getRemainingArgs();
		Path db = new Path(otherArgs1[0]);
		Path fetchlist = new Path(otherArgs1[1]);
		Path tmp = new Path(otherArgs1[2]);
		Job job = new Job(conf, "updating db");
		job.setJarByClass(fetcher.class);
		job.setMapperClass(updatingMap.class);
		job.setCombinerClass(updatingReduce.class);
		job.setReducerClass(updatingReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		if (!FileSystem.get(conf).exists(fetchlist))
			FileSystem.get(conf).mkdirs(fetchlist);
		if (!FileSystem.get(conf).exists(db))
			FileSystem.get(conf).mkdirs(db);
		FileInputFormat.addInputPath(job, fetchlist);
		FileInputFormat.addInputPath(job, db);
		if (FileSystem.get(conf).exists(tmp))
			FileSystem.get(conf).delete(tmp, true);
		FileOutputFormat.setOutputPath(job, tmp);
		try {
			job.waitForCompletion(true);
			if (FileSystem.get(conf).exists(db))
				FileSystem.get(conf).delete(db, true);
			FileSystem.get(conf).rename(tmp, db);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("updating fetchlist任务执行！！！！");
		int depth = 3;
		for (int i = 0; i < depth; i++) {
			// conf = new Configuration();
			// conf.set("mapred.job.tracker", "192.168.1.2:9001");
			String[] ars2 = new String[] { currentPath + "/crawl/fetch_list",
					currentPath + "/crawl/tmp" };
			String[] otherArgs2 = new GenericOptionsParser(conf, ars2)
					.getRemainingArgs();
			fetchlist = new Path(otherArgs2[0]);
			tmp = new Path(otherArgs2[1]);
			job = new Job(conf, "updating fetchlist");
			job.setJarByClass(fetcher.class);
			job.setMapperClass(fetchingMap.class);
			job.setCombinerClass(fetchingReduce.class);
			job.setReducerClass(fetchingReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(url_data.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.addInputPath(job, fetchlist);
			if (FileSystem.get(conf).exists(tmp))
				FileSystem.get(conf).delete(tmp, true);
			FileOutputFormat.setOutputPath(job, tmp);
			try {
				job.waitForCompletion(true);
				if (FileSystem.get(conf).exists(fetchlist))
					FileSystem.get(conf).delete(fetchlist, true);
				FileSystem.get(conf).rename(tmp, fetchlist);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("crawling 任务执行！！！！");
		// conf = new Configuration();
		// conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String[] ars3 = new String[] { currentPath + "/crawl/fetch_list",
				currentPath + "/crawl/stored_web" };
		String[] otherArgs3 = new GenericOptionsParser(conf, ars3)
				.getRemainingArgs();
		fetchlist = new Path(otherArgs3[0]);
		Path stored = new Path(otherArgs3[1]);
		job = new Job(conf, "crawling");
		job.setJarByClass(fetcher.class);
		job.setMapperClass(crawlingMap.class);
		job.setCombinerClass(crawlingReduce.class);
		job.setReducerClass(crawlingReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, fetchlist);
		FileOutputFormat.setOutputPath(job, stored);
		if (FileSystem.get(conf).exists(stored))
			FileSystem.get(conf).delete(stored, true);
		try {
			job.waitForCompletion(true);
//			if (FileSystem.get(conf).exists(fetchlist))
//				FileSystem.get(conf).delete(fetchlist, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
