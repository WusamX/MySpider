package fetch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import data_structure.url_data;

public class crawlingMap extends Mapper<Text, url_data, Text, Text> {
	
	public void map(Text arg0, url_data arg1, Context context)
	        throws IOException, InterruptedException {
		context.write(arg0, new Text("1"));
	}
}
