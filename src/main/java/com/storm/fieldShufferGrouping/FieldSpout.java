package com.storm.fieldShufferGrouping;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @Author TianCheng
 * @Date 2020/4/2 15:50
 */
public class FieldSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    private int num = 0;
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1000);
        collector.emit(new Values(num ++, num % 2));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("num", "flag"));
    }
}
