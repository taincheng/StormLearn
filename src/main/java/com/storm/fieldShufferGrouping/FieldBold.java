package com.storm.fieldShufferGrouping;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @Author TianCheng
 * @Date 2020/4/2 15:58
 */
public class FieldBold extends BaseRichBolt {
    private OutputCollector collector;
    private int sum = 0;
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int num = input.getIntegerByField("num");
        sum += num;
        System.out.println("Thread id is " + Thread.currentThread().getId()
                + ", num is " + num + ", sum is " + sum);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
