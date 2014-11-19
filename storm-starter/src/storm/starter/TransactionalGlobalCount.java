package storm.starter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This is a basic example of a transactional topology. It keeps a count of the number of tuples seen so far in a
 * database. The source of data and the databases are mocked out as in memory maps for demonstration purposes. This
 * class is defined in depth on the wiki at https://github.com/nathanmarz/storm/wiki/Transactional-topologies
 */
@SuppressWarnings("deprecation")
public class TransactionalGlobalCount {

	public static final int PARTITION_TAKE_PER_BATCH = 3;

	public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {

		private static final long serialVersionUID = -2327302921281021899L;

		{

			put(0, new ArrayList<List<Object>>() {

				private static final long serialVersionUID = -9014591785076472463L;

				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("chicken"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
				}
			});
			put(1, new ArrayList<List<Object>>() {

				private static final long serialVersionUID = 1204925485770120391L;

				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
					add(new Values("banana"));
				}
			});
			put(2, new ArrayList<List<Object>>() {

				private static final long serialVersionUID = 8269089416979940386L;

				{
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
				}
			});
		}
	};

	public static class Value {
		int count = 0;
		BigInteger txid;
	}

	public static Map<String, Value> DATABASE = new HashMap<String, Value>();
	public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

	public static class BatchCount extends BaseBatchBolt<Object> {

		private static final long serialVersionUID = 6377381170397411650L;

		Object _id;
		BatchOutputCollector _collector;

		int _count = 0;

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
			_collector = collector;
			_id = id;
		}

		@Override
		public void execute(Tuple tuple) {
			_count++;
		}

		@Override
		public void finishBatch() {
			_collector.emit(new Values(_id, _count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "count"));
		}
	}

	public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {

		private static final long serialVersionUID = 8485003494954772307L;

		TransactionAttempt _attempt;
		BatchOutputCollector _collector;

		int _sum = 0;

		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
				TransactionAttempt attempt) {
			_collector = collector;
			_attempt = attempt;
		}

		@Override
		public void execute(Tuple tuple) {
			_sum += tuple.getInteger(1);
		}

		@Override
		public void finishBatch() {
			Value val = DATABASE.get(GLOBAL_COUNT_KEY);
			Value newval;
			if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
				newval = new Value();
				newval.txid = _attempt.getTransactionId();
				if (val == null) {
					newval.count = _sum;
				} else {
					newval.count = _sum + val.count;
				}
				DATABASE.put(GLOBAL_COUNT_KEY, newval);
			} else {
				newval = val;
			}
			_collector.emit(new Values(_attempt, newval.count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "sum"));
		}
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) throws Exception {

		MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"),
				PARTITION_TAKE_PER_BATCH);
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3);
		builder.setBolt("partial-count", new BatchCount(), 5).noneGrouping("spout");
		builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial-count");

		LocalCluster cluster = new LocalCluster();

		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(3);

		cluster.submitTopology("global-count-topology", config, builder.buildTopology());

		Thread.sleep(3000);
		cluster.shutdown();
	}

}
