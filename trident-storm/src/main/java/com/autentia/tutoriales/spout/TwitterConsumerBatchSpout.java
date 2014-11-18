package com.autentia.tutoriales.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings({"serial", "rawtypes"})
public class TwitterConsumerBatchSpout implements IBatchSpout {

    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    
	@Override
    public void open(Map conf, TopologyContext context) {
		this.twitterStream = new TwitterStreamFactory().getInstance();
		this.queue = new LinkedBlockingQueue<Status>();
		
		final StatusListener listener = new StatusListener() {

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
			public void onException(Exception e) {
			}

			@Override
			public void onStallWarning(StallWarning warning) {
			}
		};

		twitterStream.addListener(listener);
		
		final FilterQuery query = new FilterQuery();
		query.track(new String[]{"realmadrid", "fcbarcelona", "atleti"});
		query.language(new String[]{"es"});

		twitterStream.filter(query);
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
    	final Status status = queue.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(status));
		}
    }

    @Override
    public void ack(long batchId) {
    }

    @Override
    public void close() {
    	twitterStream.shutdown();
    }

    @Override
    public Map getComponentConfiguration() {
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tweet");
    }
}
