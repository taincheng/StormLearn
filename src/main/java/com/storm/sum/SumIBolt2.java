package com.storm.sum;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @Author TianCheng
 * @Date 2020/3/5 17:41
 */
public class SumIBolt2 extends BaseRichBolt{
    private int total = 0;
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple input) {
        total += input.getIntegerByField("num");
        System.out.println("total : " + total);
    }
    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
