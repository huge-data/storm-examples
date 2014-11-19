package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology is a basic example of doing distributed RPC on top of Storm. It implements a function that appends a
 * "!" to any string you send the DRPC function.
 * <p/>
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more information on doing distributed RPC on top of
 * Storm.
 */
@SuppressWarnings("deprecation")
public class BasicDRPCTopology {

	public static class ExclaimBolt extends BaseBasicBolt {

		private static final long serialVersionUID = -2390118256686883865L;

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String input = tuple.getString(1);
			collector.emit(new Values(tuple.getValue(0), input + "!"));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}

	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) throws Exception {

		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
		builder.addBolt(new ExclaimBolt(), 3);

		Config conf = new Config();

		if (args == null || args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));

			for (String word : new String[] { "hello", "goodbye" }) {
				System.out.println("Result for \"" + word + "\": " + drpc.execute("exclamation", word));
			}

			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createRemoteTopology());
		}
	}

}