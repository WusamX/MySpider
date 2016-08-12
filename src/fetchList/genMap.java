package fetchList;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import data_structure.url_data;

public class genMap extends Mapper<Text, url_data, Text, url_data> {
	public void map(Text arg0, url_data arg1, Context context)
			throws IOException, InterruptedException {
		if(arg1.getStatus()==url_data.STATUS_DB_UNFETCHED) {
			arg1.setlastFetchTime(System.currentTimeMillis());
			//arg1.setFetchInterval(defInterval);
			//从输入的键值对中选择未爬取的url输出
			context.write(arg0, arg1);
		}
		else if(arg1.getStatus()==url_data.STATUS_DB_FETCHED) {
			long updateTime=arg1.getlastFetchTime()+arg1.getFetchInterval();
			if(updateTime<=System.currentTimeMillis()) {
				arg1.setStatus(url_data.STATUS_DB_UNFETCHED);
				arg1.setlastFetchTime(System.currentTimeMillis());
				//如果当前输入的键值对对应的url的爬取的上次开始爬取时间与从界面指定的超时时间间隔的和大于当前系统时间
				//则说明该爬取已经超时，需要重新爬取，将该url的状态设置成为未爬取，作为结果键值对输出
				//arg1.setFetchInterval(defInterval);
				context.write(arg0, arg1);
	        }
		}
	}

}
