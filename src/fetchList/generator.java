package fetchList;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import data_structure.url_data;
public class generator {

	public void generate() throws IOException {
		Configuration conf=new Configuration(); 
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath="/user/hadoop";
		String[] ars = new String[] { currentPath+"/crawl/db", currentPath+"/crawl/fetch_list" };
		String[] otherArgs = new GenericOptionsParser(conf, ars)
				.getRemainingArgs();
		Path in=new Path(otherArgs[0]);
		Path out=new Path(otherArgs[1]);
		Job job=new Job(conf, "generator");
		job.setJarByClass(generator.class);
		job.setMapperClass(genMap.class);
		job.setCombinerClass(genReduce.class);
		job.setReducerClass(genReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		if(!FileSystem.get(conf).exists(in))
			FileSystem.get(conf).mkdirs(in);
		FileInputFormat.addInputPath(job, in);
		if(FileSystem.get(conf).exists(out))
			FileSystem.get(conf).delete(out, true);
		FileOutputFormat.setOutputPath(job, out);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		Configuration conf=new Configuration(); 
		conf.set("mapred.job.tracker", "192.168.1.2:9001");
		String currentPath="/user/hadoop";
		String[] ars = new String[] { currentPath+"/crawl/db", currentPath+"/crawl/fetch_list" };
		String[] otherArgs = new GenericOptionsParser(conf, ars)
				.getRemainingArgs();
		Path in=new Path(otherArgs[0]);
		Path out=new Path(otherArgs[1]);
		Job job=new Job(conf, "generator");
		job.setJarByClass(generator.class);
		job.setMapperClass(genMap.class);
		job.setCombinerClass(genReduce.class);
		job.setReducerClass(genReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(url_data.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		if(!FileSystem.get(conf).exists(in))
			FileSystem.get(conf).mkdirs(in);
		FileInputFormat.addInputPath(job, in);
		if(FileSystem.get(conf).exists(out))
			FileSystem.get(conf).delete(out, true);
		FileOutputFormat.setOutputPath(job, out);
		try {
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
