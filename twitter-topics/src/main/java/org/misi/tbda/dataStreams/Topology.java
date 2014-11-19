package org.misi.tbda.dataStreams;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * To run this topology you should execute this main as: 
 * java -cp theGeneratedJar.jar twitter.streaming.Topology <track> <twitterUser> <twitterPassword>
 *
 * @author StormBook
 *
 */
public class Topology {
 
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-collector", new ApiTwitterStreamingSpoutHBC(),1);
		builder.setBolt("hashtags-sanitazer", new TwitterSanitazeHashtagsBolt()).
		shuffleGrouping("tweets-collector"); 
		builder.setBolt("hashtag-sumarizer", new TwitterSumarizeHashtagsBolt()).
			shuffleGrouping("hashtags-sanitazer"); 
		builder.setBolt("hashtag-storer", new TwitterStoreHashtagsBolt()).
		shuffleGrouping("hashtag-sumarizer");
		/*builder.setBolt("hashtag-notifier", new NotifierTwitterHashtagsBolt()).
		shuffleGrouping("hashtag-sumarizer"); */ 
	
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.put("track", "#dreamtheater");
		conf.put("user", "matiasmedeot");
		conf.put("password", "innuendo15&");
		
		cluster.submitTopology("twitter-test", conf, builder.createTopology());
	}
}
