import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.commons.lang.StringUtils;

public class PageRankMapper
	extends Mapper<LongWritable, Text, Text, Text>{
	
	//public static final double intialPR =	0.166667 ;
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			
		String line = value.toString();
		String[] pages = line.split(" ");
		String strIntialPR = pages[pages.length - 1];
		double intialPR= Double.parseDouble(strIntialPR);
		double pr = intialPR/(pages.length - 2);
		String prVal = Double.toString(pr);
		List<String> tokensPerLine = new ArrayList<String>();
		System.out.println("pr for the below keys" + prVal);
		for(int i = 1; i < pages.length - 1; i++){
			System.out.println("kry:" + pages[i]);
			context.write(new Text(pages[i]), new Text(prVal));
			tokensPerLine.add(pages[i] + " ");
		}
		System.out.println("tokens" + tokensPerLine);
		String outLink = tokensPerLine.toString();
		outLink = (outLink.substring(1, outLink.length() - 1)).replace(",","");
		System.out.println("key" + pages[0]);
		System.out.println("outlink:" + outLink);
		//String outLinks = Arrays.ttokensPerLieoString(tokensPerLine);
		context.write(new Text(pages[0]), new Text(outLink));
	}
}
