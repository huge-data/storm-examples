/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package com.mario.storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import twitter4j.*;
import com.mario.utils.Constants;
import java.io.IOException;


import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Reads Spain Tweets using the twitter4j library.
 * @author Mario
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSampleSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(2000);
		this.collector = collector;

		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

                        @Override
                        public void onStallWarning(StallWarning stallWarning) {
                        }

                        @Override
			public void onException(Exception e) {
			}
		};
                
                //Twitter stream authentication setup
                final Properties properties = new Properties();
                try {
                        properties.load(TwitterSampleSpout.class.getClassLoader().getResourceAsStream(Constants.CONFIG_PROPERTIES_FILE));
                } catch (final IOException ioException) {
                        //Should not occur. If it does, we cant continue. So exiting the program!
                        System.exit(1);
                }

                final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
                configurationBuilder.setIncludeEntitiesEnabled(true);

                configurationBuilder.setOAuthAccessToken(properties.getProperty(Constants.OAUTH_ACCESS_TOKEN));
                configurationBuilder.setOAuthAccessTokenSecret(properties.getProperty(Constants.OAUTH_ACCESS_TOKEN_SECRET));
                configurationBuilder.setOAuthConsumerKey(properties.getProperty(Constants.OAUTH_CONSUMER_KEY));
                configurationBuilder.setOAuthConsumerSecret(properties.getProperty(Constants.OAUTH_CONSUMER_SECRET));
                
                TwitterStreamFactory factory = new TwitterStreamFactory(configurationBuilder.build());
		twitterStream = factory.getInstance();
		twitterStream.addListener(listener);
                
                final FilterQuery filterQuery = new FilterQuery();
       
                final double[][] boundingBoxOfSpain = {{-18.1590, 27.6363},{4.3279, 43.7900}};
                filterQuery.locations(boundingBoxOfSpain);
                twitterStream.filter(filterQuery);
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
        } else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
