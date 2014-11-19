package trident.storm.tutoriales;

import java.io.IOException;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;
import trident.storm.tutoriales.functions.HashtagExtractor;
import trident.storm.tutoriales.spout.TwitterConsumerBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class TrendingTopicsTridentTopology {

	public static StormTopology createTopology(IBatchSpout spout) throws IOException {
		final TridentTopology topology = new TridentTopology();

		topology.newStream("spout", spout).each(new Fields("tweet"), new HashtagExtractor(), new Fields("hashtag"))
				.groupBy(new Fields("hashtag"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).newValuesStream()
				.each(new Fields("hashtag", "count"), new Debug());

		return topology.build();
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		final Config conf = new Config();
		final LocalCluster local = new LocalCluster();
		final IBatchSpout spout = new TwitterConsumerBatchSpout();

		try {
			local.submitTopology("hashtag-count-topology", conf, createTopology(spout));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
