package com.storm.wordCount;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author TianCheng
 * @Date 2020/3/6 11:02
 */
public class WountCountBolt extends BaseRichBolt {

    private ConcurrentHashMap<String, Integer> wordCount;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        String split = input.getStringByField("split");
        split = StringUtils.strip(split, " .:,\"").toLowerCase();

        wordCount.merge(split, 1, (oldValue, newValue) -> oldValue + newValue);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
