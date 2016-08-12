package injector;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import data_structure.url_data;

public class injector {
	public void inject(long interval) throws IOException {

		//String currentPath=System.getProperty("user.dir");
		System.out.println("第一个任务执行！！！！");
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath="/user/hadoop";
		String[] ars1 = new String[] { currentPath+"/crawl/input", currentPath+"/crawl/inject" };
		String[] otherArgs1 = new GenericOptionsParser(conf, ars1)
				.getRemainingArgs();
		//以下代码用来组合文件路径字符串
		Path input=new Path(otherArgs1[0]);
		Path inject=new Path(otherArgs1[1]);
		Job job = new Job(conf, "injector");
		job.setJarByClass(injector.class);
		// specify a mapper
		job.setMapperClass(injectorMap.class);
		// specify a combiner
		job.setCombinerClass(injectorReduce.class);
        // specify a reducer
		job.setReducerClass(injectorReduce.class);
		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(input))
			FileSystem.get(conf).mkdirs(input);
		FileInputFormat.addInputPath(job, input);
		if(FileSystem.get(conf).exists(inject));
			FileSystem.get(conf).delete(inject, true);
		FileOutputFormat.setOutputPath(job, inject);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("第一个任务完成！！！！");
		//以上部分是用mapreduce分析文本文件中的url，作为入口地址存入到inject目录下的文件中
		//以下的是新建一个任务，用来更新分析原始文本文件中的url后保存在目录inject还有db中的文件
		System.out.println("第二个任务开始！！！！");
		conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String[] ars2 = new String[] { currentPath+"/crawl/db", currentPath+"/crawl/new", currentPath+"/crawl/inject" };
		String[] otherArgs2 = new GenericOptionsParser(conf, ars2)
				.getRemainingArgs();
		Path dbpath=new Path(otherArgs2[0]);
		Path tmppath=new Path(otherArgs2[1]);
		inject=new Path(otherArgs2[2]);
		job = new Job(conf, "updater");
		job.setJarByClass(injector.class);
		// specify a mapper
		job.setMapperClass(updateMap.class);
		// specify a combiner
		job.setCombinerClass(updateReduce.class);
		// specify a reducer
		job.setReducerClass(updateReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(dbpath))
			FileSystem.get(conf).mkdirs(dbpath);
		FileInputFormat.addInputPath(job, inject);
		FileInputFormat.addInputPath(job, dbpath);
		//输入路径是db和inject
		if(FileSystem.get(conf).exists(tmppath))
		  FileSystem.get(conf).delete(tmppath, true);
		FileOutputFormat.setOutputPath(job, tmppath);
        //输出路径是tmppath也就是new目录

		try {
			job.waitForCompletion(true);
			if(FileSystem.get(conf).exists(tmppath)) {
				if(FileSystem.get(conf).exists(dbpath)) {
					FileSystem.get(conf).delete(dbpath, true);
					FileSystem.get(conf).rename(tmppath, dbpath);
					//运行完成后将tmppath（new）目录修改成dbpath（db）
				}
			}
			if(FileSystem.get(conf).exists(inject))
				FileSystem.get(conf).delete(inject, true);
			//删除inject目录，最终分析完的url信息都保存到了dbpath（db）
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("第二个任务完成！！！！");
	}


	public static void main(String[] args) throws IOException {
		//String currentPath=System.getProperty("user.dir");
		System.out.println("第一个任务执行！！！！");
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath="/user/hadoop";
		String[] ars1 = new String[] { currentPath+"/crawl/input", currentPath+"/crawl/inject" };
		String[] otherArgs1 = new GenericOptionsParser(conf, ars1)
				.getRemainingArgs();
		//以下代码用来组合文件路径字符串
		Path input=new Path(otherArgs1[0]);
		Path inject=new Path(otherArgs1[1]);
		Job job = new Job(conf, "injector");
		job.setJarByClass(injector.class);
		// specify a mapper
		job.setMapperClass(injectorMap.class);
		// specify a combiner
		job.setCombinerClass(injectorReduce.class);
        // specify a reducer
		job.setReducerClass(injectorReduce.class);
		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(input))
			FileSystem.get(conf).mkdirs(input);
		FileInputFormat.addInputPath(job, input);
		if(FileSystem.get(conf).exists(inject));
			FileSystem.get(conf).delete(inject, true);
		FileOutputFormat.setOutputPath(job, inject);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("第一个任务完成！！！！");
		//以上部分是用mapreduce分析文本文件中的url，作为入口地址存入到inject目录下的文件中
		//以下的是新建一个任务，用来更新分析原始文本文件中的url后保存在目录inject还有db中的文件
		System.out.println("第二个任务开始！！！！");
		conf = new Configuration();
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String[] ars2 = new String[] { currentPath+"/crawl/db", currentPath+"/crawl/new", currentPath+"/crawl/inject" };
		String[] otherArgs2 = new GenericOptionsParser(conf, ars2)
				.getRemainingArgs();
		Path dbpath=new Path(otherArgs2[0]);
		Path tmppath=new Path(otherArgs2[1]);
		inject=new Path(otherArgs2[2]);
		job = new Job(conf, "updater");
		job.setJarByClass(injector.class);
		// specify a mapper
		job.setMapperClass(updateMap.class);
		// specify a combiner
		job.setCombinerClass(updateReduce.class);
		// specify a reducer
		job.setReducerClass(updateReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(dbpath))
			FileSystem.get(conf).mkdirs(dbpath);
		FileInputFormat.addInputPath(job, inject);
		FileInputFormat.addInputPath(job, dbpath);
		//输入路径是db和inject
		if(FileSystem.get(conf).exists(tmppath))
		  FileSystem.get(conf).delete(tmppath, true);
		FileOutputFormat.setOutputPath(job, tmppath);
        //输出路径是tmppath也就是new目录

		try {
			job.waitForCompletion(true);
			if(FileSystem.get(conf).exists(tmppath)) {
				if(FileSystem.get(conf).exists(dbpath)) {
					FileSystem.get(conf).delete(dbpath, true);
					FileSystem.get(conf).rename(tmppath, dbpath);
					//运行完成后将tmppath（new）目录修改成dbpath（db）
				}
			}
			if(FileSystem.get(conf).exists(inject))
				FileSystem.get(conf).delete(inject, true);
			//删除inject目录，最终分析完的url信息都保存到了dbpath（db）
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("第二个任务完成！！！！");
	}
}

