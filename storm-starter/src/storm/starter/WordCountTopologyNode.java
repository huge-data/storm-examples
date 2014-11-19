package storm.starter;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.ShellSpout;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopologyNode {

	public static class SplitSentence extends ShellBolt implements IRichBolt {

		private static final long serialVersionUID = -2806393499317371167L;

		public SplitSentence() {
			super("node", "splitsentence.js");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
	}

	public static class RandomSentence extends ShellSpout implements IRichSpout {

		private static final long serialVersionUID = -3838759230590744395L;

		public RandomSentence() {
			super("node", "randomsentence.js");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}
	}

	public static class WordCount extends BaseBasicBolt {

		private static final long serialVersionUID = -2581980761500023948L;

		Map<String, Integer> counts = new HashMap<String, Integer>();

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String word = tuple.getString(0);
			Integer count = counts.get(word);
			if (count == null)
				count = 0;
			count++;
			counts.put(word, count);
			collector.emit(new Values(word, count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new RandomSentence(), 5);

		builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
		builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}

}
