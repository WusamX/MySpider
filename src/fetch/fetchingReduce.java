package fetch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import data_structure.url_data;

public class fetchingReduce extends Reducer<Text, url_data, Text, url_data> {
	
	public void reduce(Text arg0, Iterable<url_data> arg1,
			Context context) throws IOException, InterruptedException {
		url_data data=new url_data();
		while(arg1.iterator().hasNext()) {
			data.set(arg1.iterator().next());
			//这部分有一个默认的排序，使用在iterator最后面的那个url_data赋值
			//因为UNFETCHED、FETCHED、READYTOFETCHED的值依次增大，存最靠近结果最不需要继续爬的的状态
		}
		context.write(arg0, data);
	}
}
