package org.misi.tbda.dataStreams;



import java.net.URI;
import java.util.Map;

import org.misi.tbda.dataStreams.notifiers.NotifierWebSocket;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class NotifierTwitterHashtagsBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;
	NotifierWebSocket  notifierWebSocket;
	String clientname = "trendTopicNotifier";
	String protocol = "ws";
	String host = "localhost";
	int port = 8080;
	String serverlocation = protocol + "://" + host + ":" + port;
	URI uri = null;

	
	
	@Override
	public void cleanup() {
		
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = (String) input.getValueByField("hashtag");
		String hashtagFrecuency = (String) input.getValueByField("frequencyValue");
	    notifierWebSocket.send(hashtag +"-"+hashtagFrecuency);
	     
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		uri = URI.create( serverlocation + "/twitterTrendTopicsFrontEnd/dataStream?clientName=" + clientname );
		notifierWebSocket = new NotifierWebSocket();
		notifierWebSocket.connect(uri.toString());
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
