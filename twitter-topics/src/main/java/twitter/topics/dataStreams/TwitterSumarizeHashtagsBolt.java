package twitter.topics.dataStreams;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterSumarizeHashtagsBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 8077994610194120075L;

	Map<String, Integer> hashtags = new HashMap<String, Integer>();

	@Override
	public void cleanup() {
		//
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String hashtag = (String) input.getValueByField("hashtag");
		if (!hashtag.isEmpty()) {
			Integer frequencyValue = 1;
			if (!hashtags.containsKey(hashtag)) {
				hashtags.put(hashtag, frequencyValue);
			} else {
				Integer last = hashtags.get(hashtag);
				frequencyValue = last + 1;
				hashtags.put(hashtag, frequencyValue);
			}
			collector.emit(new Values(hashtag, frequencyValue.toString()));
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				Map<String, Integer> oldMap = new HashMap<String, Integer>(hashtags);
				hashtags.clear();
				for (Map.Entry<String, Integer> entry : oldMap.entrySet()) {
					System.out.println(entry.getKey() + ": " + entry.getValue());
				}
			}

		};
		Timer t = new Timer();
		t.scheduleAtFixedRate(task, 60000, 60000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag", "frequencyValue"));
	}

}
