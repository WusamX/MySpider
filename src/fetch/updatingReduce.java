package fetch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import data_structure.url_data;

public class updatingReduce extends Reducer<Text, url_data, Text, url_data> {
	
	public void reduce(Text arg0, Iterable<url_data> arg1,
			Context context) throws IOException, InterruptedException {
		url_data tmp=new url_data();
		while(arg1.iterator().hasNext()) {
			tmp.set(arg1.iterator().next());
		}
		tmp.setlastFetchTime(System.currentTimeMillis());
		tmp.setStatus(url_data.STATUS_DB_FETCHED);
		//将db中所有的url都标记成为FETCHED
		context.write(arg0, tmp);
	}

}
