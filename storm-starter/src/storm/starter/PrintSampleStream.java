package storm.starter;

import java.util.Arrays;

import storm.starter.bolt.PrinterBolt;
import storm.starter.spout.TwitterSampleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class PrintSampleStream {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret,
				keyWords));
		builder.setBolt("print", new PrinterBolt()).shuffleGrouping("twitter");

		Config conf = new Config();

		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("test", conf, builder.createTopology());

		Utils.sleep(10000);
		cluster.shutdown();
	}

}
