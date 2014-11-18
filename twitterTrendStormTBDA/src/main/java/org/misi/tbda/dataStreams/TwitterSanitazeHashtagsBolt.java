package org.misi.tbda.dataStreams;

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterSanitazeHashtagsBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;
	
	@Override
	public void cleanup() {
		
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		JSONObject json = (JSONObject)input.getValueByField("tweet");
		if(json.containsKey("entities")){
			JSONObject entities = (JSONObject) json.get("entities");
				if(entities.containsKey("hashtags")){
					for(Object hashObj : (JSONArray)entities.get("hashtags")){
						JSONObject hashJson = (JSONObject)hashObj;
						String hashtag = hashJson.get("text").toString().toLowerCase();
						if(hashtag.matches("^[\\w\\s-]*$")) {
							collector.emit(new Values(hashtag)); 
						}
					}
			}
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag"));
	}

}
