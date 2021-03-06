package com.storm.wordCount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @Author TianCheng
 * @Date 2020/3/5 20:26
 */
public class WordCountTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout(), 1);
        builder.setBolt("SplitBolt", new SplitBolt(), 1).shuffleGrouping("DataSourceSpout");
        builder.setBolt("WountCountBolt", new WountCountBolt(), 1).shuffleGrouping("SplitBolt");
        try {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCountTopology", new Config(), builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
