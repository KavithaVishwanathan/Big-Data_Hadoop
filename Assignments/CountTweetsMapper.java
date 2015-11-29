import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountTweetsMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public String[] tweets = { "chicago", "hackathon", "dec", "java" };
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String lineCaps = line.toLowerCase();
		int size = tweets.length;
		boolean found;
		for (int i =0; i<size; i++){
			found = lineCaps.contains(tweets[i]);
			if (found) {
				context.write(new Text(tweets[i]), new IntWritable(1));
			} else {
				context.write(new Text(tweets[i]), new IntWritable(0));
			}
		}
	}
}
