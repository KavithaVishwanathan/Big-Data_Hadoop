import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.ArrayList;

public class PageRankReducer
extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		List<String> vArrayList = new ArrayList<String>();
    	
    	for(Text v : values) {
    		System.out.println("v:" + v);
    		vArrayList.add(v.toString());
    	}
		double PR = 0;
		String page = "";
		for (int i=0; i < vArrayList.size(); i++) {
			String outLink = vArrayList.get(i);
			System.out.println("outlink:" + outLink);
			if (outLink.matches(".*[1-9].*")) {
				double prValue = Double.parseDouble(outLink);
				PR = PR + prValue;
				System.out.println("PR:" + PR);
			} else {
				page = outLink;
				System.out.println("page:" + page);
			}
		}
		System.out.println("key+page+PR" + key + page + PR);
		//System.out.println(PR);
		context.write(new Text(key + " " + page), new DoubleWritable(PR));
	}
}