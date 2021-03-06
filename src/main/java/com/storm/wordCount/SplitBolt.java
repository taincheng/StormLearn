package com.storm.wordCount;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Author TianCheng
 * @Date 2020/3/5 20:58
 */
public class SplitBolt extends BaseRichBolt{
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String tmp = input.getStringByField("line").trim();
        if (StringUtils.isNotBlank(tmp)) {
            String[] split = StringUtils.split(tmp, null, 0);
            if (split.length != 0){
                for (String s : split){
                    collector.emit(new Values(s));
                }
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("split"));
    }
}
