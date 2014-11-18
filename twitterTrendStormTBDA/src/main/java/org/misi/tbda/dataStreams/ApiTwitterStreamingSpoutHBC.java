package org.misi.tbda.dataStreams;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ApiTwitterStreamingSpoutHBC extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
	BasicClient client;
	private SpoutOutputCollector collector;
	private String track;

	static Logger LOG = Logger.getLogger(ApiTwitterStreamingSpoutHBC.class);
	static JSONParser jsonParser = new JSONParser();

	@Override
	public void nextTuple() {

		if (!client.isDone()) {
			try {
				String msg = queue.poll(5, TimeUnit.SECONDS);
				if (msg != null) {
					Object json = jsonParser.parse(msg);
					collector.emit(new Values(track, json));
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}

	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		int spoutsSize = context
				.getComponentTasks(context.getThisComponentId()).size();
		int myIdx = context.getThisTaskIndex();
		String[] tracks = ((String) conf.get("track")).split(",");
		StringBuffer tracksBuffer = new StringBuffer();
		for (int i = 0; i < tracks.length; i++) {
			if (i % spoutsSize == myIdx) {
				tracksBuffer.append(",");
				tracksBuffer.append(tracks[i]);
			}
		}

		if (tracksBuffer.length() == 0)
			throw new RuntimeException("No track found for spout"
					+ " [spoutsSize:" + spoutsSize + ", tracks:"
					+ tracks.length + "] the amount"
					+ " of tracks must be more then the spout paralellism");

		this.track = "#dreamtheater";
		this.collector = collector;
		try {
			oauth("6vmb7wKsiPXNoKgGdevDg",
					"ZrgBsYLzZ7ojj5ezlmjAAoUq0ehBFmF1tAw6c4W25U",
					"452618044-hvKCDbM3vFTEPH1HTnh6ejXHCzIwUOA9gw4QEkG8",
					"4otKVfaf14taWVL0M77i3fs9DJcrxEDaydz2CyACVo");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("criteria", "tweet"));
	}

	@Override
	public void close() {
		client.stop();
	}

	public void oauth(String consumerKey, String consumerSecret, String token,
			String secret) throws InterruptedException {

//		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
//		endpoint.trackTerms(Lists.newArrayList(this.track));

		StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
		endpoint.stallWarnings(false);

		    
		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);

		client = new ClientBuilder().name("sampleExampleClient")
				.hosts(Constants.STREAM_HOST).endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();
		client.connect();
	}

}
