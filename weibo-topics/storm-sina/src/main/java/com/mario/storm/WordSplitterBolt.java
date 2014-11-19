package com.mario.storm;

import java.util.Map;

import twitter4j.GeoLocation;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordSplitterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6284536822289357622L;

	private final int minWordLength;

	private OutputCollector collector;

	public WordSplitterBolt(int minWordLength) {
		this.minWordLength = minWordLength;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		String lang = tweet.getUser().getLang();
		GeoLocation loc = tweet.getGeoLocation();
		String text = tweet.getText().replaceAll("\\p{Punct}", " ").toLowerCase();
		String[] words = text.split(" ");
		for (String word : words) {
			if (word.length() >= minWordLength) {
				collector.emit(new Values(lang, word, loc));
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lang", "word", "loc"));
	}

}
