package fetchList;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import data_structure.url_data;

public class genReduce extends Reducer<Text, url_data, Text, url_data> {

	public void reduce(Text arg0, Iterable<url_data> arg1,
			Context context) throws IOException, InterruptedException {
		url_data data=new url_data();
		long recent=0;
		while(arg1.iterator().hasNext()) {
			url_data tmp=new url_data();
			tmp.set(arg1.iterator().next());
			if(recent<tmp.getlastFetchTime()) {
				recent=tmp.getlastFetchTime();
				data.set(tmp);
			}
			//以上这部分循环主要是用选择排序的方式找出当前url最近一次爬取的状态信息，再一次去重，去掉那些相同url并且都超时的重复url，
			//只保留其中离当前时间最近一次的爬取保存作为输出的键值对
		}
		if(recent!=0)
			context.write(arg0, data);
	}
}
