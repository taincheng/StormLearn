package com.storm.fieldShufferGrouping;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @Author TianCheng
 * @Date 2020/4/2 15:48
 */
public class FieldTopolgies {
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new FieldSpout(), 2);
        builder.setBolt("bolt", new FieldBold(), 3)
                .fieldsGrouping("spout",new Fields("flag"));
        Config config = new Config();
        config.setNumWorkers(2);
        try {
            StormSubmitter.submitTopology(builder.getClass().getSimpleName(), config, builder.createTopology());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
