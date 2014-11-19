package org.misi.tbda.dataStreams;

import java.util.Map;

import org.misi.tbda.dataStreams.dominio.Topic;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.google.gson.Gson;

public class TwitterStoreHashtagsBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;
	Jedis jedis;
	Topic topic;
	Gson gson;
	
	
	
	@Override
	public void cleanup() {
		
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = (String) input.getValueByField("hashtag");
		String hashtagFrecuency = (String) input.getValueByField("frequencyValue");
		Integer freq = new Integer(hashtagFrecuency);
		if(freq>1) {
			topic.setHashtag(hashtag);
			topic.setFrecuency(hashtagFrecuency);
			String json = gson.toJson(topic); 
			jedis.rpush("topics", json);
		}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	 jedis = new Jedis("127.0.0.1",6379);	
	 topic = new Topic();
     gson = new Gson();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
