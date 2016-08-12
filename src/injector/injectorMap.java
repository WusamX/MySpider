package injector;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import data_structure.url_data;

public class injectorMap extends Mapper<Object, Text, Text, url_data> {
	long interval;
	
	public void map(Object key, Text values, Context context)
			throws IOException, InterruptedException {
		//将文本文件中的url以行切割，将行号作为键，每一行的字符作为值
		String url=values.toString();
		if(url!=null) {
			//data用来记录一个url的相关信息
			url_data data=new url_data(url_data.STATUS_INJECTED);
			data.setFetchInterval(this.interval);
			values.set(url);
			//输出以url作为键，该url的相关信息作为值
			context.write(values, data);
		}
	}
	
	protected void setup(Mapper.Context context)
            throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		// 从作业的配置对象中获得interval，根源是从界面输入的
		interval=conf.getLong("interval", 1000*3600*24);
	}

}
