package com.mario.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import redis.clients.jedis.Jedis;
import twitter4j.GeoLocation;

/**
 * Keeps stats on word count, calculates and logs top words every X second to stdout and top list every Y seconds,
 * @author mario
 */
public class WordCounterBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);
    /** Number of seconds before the top list will be logged to stdout. */
    private final long logIntervalSec;
    /** Number of seconds before the top list will be cleared. */
    private final long clearIntervalSec;
    /** Number of top words to store in stats. */
    private final int topListSize;

    private Map<String, Long> counter;
    private Map<String, GeoLocation> location;
    private long lastLogTime;
    private long lastClearTime;
    private Jedis jedis;

    public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        counter = new HashMap<String, Long>();
        location = new HashMap<String, GeoLocation>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
        jedis = new Jedis("localhost");
       
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
        if (null == input.getValueByField("loc")) return;
        String word = (String) input.getValueByField("word");
        GeoLocation loc = (GeoLocation) input.getValueByField("loc");
        Long count = counter.get(word);
        count = count == null ? 1L : count + 1;
        counter.put(word, count);
        location.put(word, loc);

//        logger.info(new StringBuilder(word).append('>').append(count).toString());

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
            logger.info("Word count: "+counter.size());

            publishTopList();
            lastLogTime = System.currentTimeMillis();
        }
    }

    private void publishTopList() {
        // calculate top list:
        int i = 0;
        Map<String, String> map = new HashMap<String, String>(); 
       
        SortedMap<Long, String> top = new TreeMap<Long, String>();
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > topListSize) {
                location.remove(top.get(top.firstKey()));
                top.remove(top.firstKey());
            }
        }

        // Output top list:
        for (Map.Entry<Long, String> entry : top.entrySet()) {
            try{
                i++;
                map.put("text", entry.getValue());
                map.put("lat",  String.valueOf(location.get(entry.getValue()).getLatitude()));
                map.put("long", String.valueOf(location.get(entry.getValue()).getLongitude()));
                map.put("count", String.valueOf(entry.getKey()));
                jedis.hmset("count:" + i, map );
            }catch(Exception e){
                logger.info("error: not location");
            }
            logger.info(new StringBuilder("top - ").append(entry.getValue()).append('>').append(entry.getKey()).toString());
        }
        
        //publish results toString
        jedis.publish("tweets", Integer.toString(i));
        
        // Clear top list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
            counter.clear();
            location.clear();
            lastClearTime = now;
        }
    }
}
