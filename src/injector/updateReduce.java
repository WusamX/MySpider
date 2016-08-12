package injector;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import data_structure.url_data;

public class updateReduce extends Reducer<Text, url_data, Text, url_data> {
	
	public void reduce(Text arg0, Iterable<url_data> arg1,
			Context context) throws IOException, InterruptedException {
		url_data old=new url_data();
		//old只有两种状态，空白或者是FETCHED
		url_data newd=new url_data();
		while(arg1.iterator().hasNext()) {
			url_data ud=arg1.iterator().next();
			if(ud.getStatus()==url_data.STATUS_DB_FETCHED) {
				old.set(ud);
				//从同一个url的迭代器中遍历所有的抓取状态信息只要发现当前的url已经被抓取过了的标记就将是否抓取的状态信息存储为FETCHED，结束对当前这个url的遍历
				break;
			}else {
				newd.set(ud);
				//如果一直没有发现这个url有被抓取的标记，就新建一个url_data保存当前的url状态，并在下面的代码中设置为UNFETCHED
			}
		}
		if(old.getStatus()!=0)
			context.write(arg0, old);
		else {
			newd.setStatus(url_data.STATUS_DB_UNFETCHED);
			context.write(arg0, newd);
		}		
	}

}
