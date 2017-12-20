package bigdatateam.burstyword;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BurstyWordTopology {
    public static class SplitSentenceBolt extends ShellBolt implements IRichBolt {
  
      public SplitSentenceBolt() {
        super("python3", "split.py");
      }
  
      @Override
      public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","ts2"));
      }
  
      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }
    }

    public static class PawooSentenceSpout extends ShellSpout implements IRichSpout {

      public PawooSentenceSpout() {
        super("python3", "realspout.py");
      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("sentence","ts1"));
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
          return null;
      }

    }
  
    public static class RedisIOBolt extends ShellBolt implements IRichBolt {
  
      public RedisIOBolt() {
        super("python3", "redisio.py");
      }
  
      @Override
      public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // to be checked
        declarer.declare(new Fields("shit"));
      }
  
      @Override
      public Map<String, Object> getComponentConfiguration() {
        return null;
      }
    }
  
    public static void main(String[] args) throws Exception {
      
  
      TopologyBuilder builder = new TopologyBuilder();
  
      builder.setSpout("spout", new PawooSentenceSpout(), 5);
  
      builder.setBolt("split", new SplitSentenceBolt(), 20).shuffleGrouping("spout");
      builder.setBolt("count", new RedisIOBolt(), 8).shuffleGrouping("split");
  
      //conf.setDebug(true);
  
      //String topologyName = "word-count";
  
      //conf.setNumWorkers(3);
  
      Config conf = new Config();
      conf.setDebug(true);

      if (args != null && args.length > 0) {
        conf.setNumWorkers(3);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
      }
      else {
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());

        Thread.sleep(10000);

        cluster.shutdown();
      }
    }
  }
