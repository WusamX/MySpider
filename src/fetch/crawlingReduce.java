package fetch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import data_structure.url_data;

public class crawlingReduce extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text arg0, Iterable<url_data> arg1,
			Context context) throws IOException, InterruptedException {
		// 该部分仅仅是将fetchlist中的url去重后统计个数然后输出到stored的目录下，
		//如果想对网页的内容作进一步的处理，可以在该处做处理，这时的fetchlist中是本次抓取过程中所有的url
		int sum=0;
		while(arg1.iterator().hasNext()) {
			sum+=Integer.parseInt(arg1.iterator().next().toString());
		}
		String s=""+sum;
		context.write(new Text(s), arg0);
	}
}
