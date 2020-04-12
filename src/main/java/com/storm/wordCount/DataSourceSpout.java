package com.storm.wordCount;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @Author TianCheng
 * @Date 2020/3/5 20:29
 */
public class DataSourceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void nextTuple() {
        Collection<File> files = FileUtils.listFiles(new File("src/main/resources/file"), new String[]{"txt"}, true);
        if(!files.isEmpty()){
            for (File file : files){
                try {
                    List<String> lines = FileUtils.readLines(file, "utf-8");
                    for (String line : lines){
                        collector.emit(new Values(line));
                    }
                    FileUtils.moveFile(file, new File(file.getPath() + "1"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
