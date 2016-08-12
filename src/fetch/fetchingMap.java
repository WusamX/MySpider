package fetch;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import data_structure.url_data;

public class fetchingMap extends Mapper<Text, url_data, Text, url_data> {
	
	private ArrayList<String> urls=new ArrayList<String>();
	
	public void map(Text arg0, url_data arg1, Context context)
	        throws IOException, InterruptedException {
		if(arg1.getStatus()==url_data.STATUS_DB_UNFETCHED) {
			arg1.setStatus(url_data.STATUS_DB_READYTOFETCH);
			context.write(arg0, arg1);
			urls=HerfMatch.FindURL(arg0.toString());
			if(urls==null)
				return;
			for(int i=0;i<urls.size();i++) {
				url_data tmp=new url_data();
				String url=urls.get(i);
				tmp.set(arg1);
				tmp.setStatus(url_data.STATUS_DB_UNFETCHED);
				tmp.setlastFetchTime(System.currentTimeMillis());
				context.write(new Text(url), tmp);
			}
		}else {
			context.write(arg0, arg1);
		}
	}
}
