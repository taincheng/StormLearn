package com.storm.sum;

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
 * @Date 2020/3/5 17:41
 */
public class SumIBolt extends BaseRichBolt{
    private OutputCollector collector;
    /**
     * Called when a task for this component is initialized within a worker on the cluster. It provides the bolt with the environment in
     * which the bolt executes.
     * <p>
     * <p>This includes the:
     *
     * @param topoConf  The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster
     *                  configuration on this machine.
     * @param context   This object can be used to get information about this task's place within the topology, including the task id and
     *                  component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and
     */
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * Process a single tuple of input. The Tuple object contains metadata on it about which component/stream/task it came from. The values
     * of the Tuple can be accessed using Tuple#getValue. The IBolt does not have to process the Tuple immediately. It is perfectly fine to
     * hang onto a tuple and process it later (for instance, to do an aggregation or join).
     * <p>
     * <p>Tuples should be emitted using the OutputCollector provided through the prepare method. It is required that all input tuples are
     * acked or failed at some point using the OutputCollector. Otherwise, Storm will be unable to determine when tuples coming off the
     * spouts have been completed.
     * <p>
     * <p>For the common case of acking an input tuple at the end of the execute method, see IBasicBolt which automates this.
     *
     * @param input The input tuple to be processed.
     */
    @Override
    public void execute(Tuple input) {
        collector.emit(input, new Values(input.getIntegerByField("num")));
        collector.ack(input);
    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("num"));
    }
}
