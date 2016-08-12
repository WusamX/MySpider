package injector;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import data_structure.url_data;

public class injectorReduce extends Reducer<Text, url_data, Text, url_data> {

	public void reduce(Text _key, Iterable<url_data> values,
			Context context) throws IOException, InterruptedException {
		// replace KeyType with the real type of your key
		Text key = (Text) _key;
		url_data newdata=new url_data();
		while (values.iterator().hasNext()) {
			// replace ValueType with the real type of your value
			url_data value = values.iterator().next();
			if(value.getStatus()==url_data.STATUS_INJECTED) {
				newdata.set(value);
				newdata.setStatus(url_data.STATUS_DB_UNFETCHED);
			}
		}
		//以上部分主要是合并具有相同url的键值对，并将状态有INJECTED的改成UNFETCHED,输出格式是以url为键，url的相关信息为值
		context.write(key, newdata);
	}

}
