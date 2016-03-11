import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class TwitterFeeds {

	private static final String CONSUMER_KEY="";
	private static final String CONSUMER_SECRET="";
	private static final String ACCESS_TOKEN="";
	private static final String ACCESS_TOKEN_SECRET="";
	
	private TwitterStream twitterStream;
	private String tweetWords[] = {"hadoop", "big Data"};
	FileOutputStream fos;
	
		
	public TwitterFeeds() {
 
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(CONSUMER_KEY);
		cb.setOAuthConsumerSecret(CONSUMER_SECRET);
		cb.setOAuthAccessToken(ACCESS_TOKEN);
		cb.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
		cb.setJSONStoreEnabled(true);
		cb.setIncludeEntitiesEnabled(true);
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	}

	public void startTwitter() {

		fos = new FileOutputStream(new File("E:\\Kavi\\tweets.json"));
		twitterStream.addListener(listener);
		// Getting the tweets based on the query given
		FilterQuery query = new FilterQuery().track(tweetWords);
		twitterStream.filter(query);

	}

	public void closeTwitter() {
		twitterStream.shutdown();
			fos.close();
	}

	StatusListener listener = new StatusListener() {

		public void onStatus(Status status) {
			System.out.println(status.getUser().getScreenName() + ": " + status.getText());
			System.out.println(String.valueOf(status.getCreatedAt().getTime()));
			fos.write(DataObjectFactory.getRawJSON(status).getBytes());
		}
	};

	public static void main(String[] args) throws InterruptedException {

		TwitterFeeds twitter = new TwitterFeeds();
		twitter.startTwitter();
		Thread.sleep(20000);
		twitter.closeTwitter();

	}

}