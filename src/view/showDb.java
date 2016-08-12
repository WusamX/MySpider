package view;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import data_structure.url_data;

public class showDb extends Mapper<Text, url_data, Text, url_data> {
	
	public void map(Text arg0, url_data arg1, Context context)
			throws IOException, InterruptedException {
		context.write(arg0, arg1);
	}
	
	public void show() throws IOException {
		Configuration conf=new Configuration(); 
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath="/user/hadoop";
		String[] ars = new String[] { currentPath+"/crawl/db", currentPath+"/crawl/dbviewer" };
		String[] otherArgs = new GenericOptionsParser(conf, ars)
				.getRemainingArgs();
		Path db=new Path(otherArgs[0]);
		Path view=new Path(otherArgs[1]);
		Job job=new Job(conf, "dbviewer");
		job.setJarByClass(showDb.class);
		job.setMapperClass(showDb.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, db);
		
		if(FileSystem.get(conf).exists(view))
			FileSystem.get(conf).delete(view, true);
		FileOutputFormat.setOutputPath(job, view);
		try{
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Configuration conf=new Configuration(); 
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath="/user/hadoop";
		String[] ars = new String[] { currentPath+"/crawl/fetch_list", currentPath+"/crawl/dbviewer" };
		String[] otherArgs = new GenericOptionsParser(conf, ars)
				.getRemainingArgs();
		Path db=new Path(otherArgs[0]);
		Path view=new Path(otherArgs[1]);
		Job job=new Job(conf, "dbviewer");
		job.setJarByClass(showDb.class);
		job.setMapperClass(showDb.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, db);
		
		if(FileSystem.get(conf).exists(view))
			FileSystem.get(conf).delete(view, true);
		FileOutputFormat.setOutputPath(job, view);
		try{
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
