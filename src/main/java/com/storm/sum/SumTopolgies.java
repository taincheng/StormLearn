package com.storm.sum;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @Author TianCheng
 * @Date 2020/3/5 17:40
 */
public class SumTopolgies {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SumSpout", new SumISpout(), 1);
        builder.setBolt("SumBolt1", new SumIBolt(), 1).shuffleGrouping("SumSpout");
        builder.setBolt("SumBolt2", new SumIBolt2(), 1).shuffleGrouping("SumBolt1");

        //创建本地Storm模式
        try {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("SumTopology", new Config(), builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
