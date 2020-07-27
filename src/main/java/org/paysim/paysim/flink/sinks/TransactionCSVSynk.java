package org.paysim.paysim.flink.sinks;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.paysim.paysim.base.Transaction;
import org.paysim.paysim.flink.consumers.TransactionConsumer;

public class TransactionCSVSynk {

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.enableCheckpointing(1000);

    String output = params.get("csv_output_path");

    DataStream<Transaction> stream = TransactionConsumer
      .run(env, params.get("brokers", "localhost:9092"), params.get("topic", "transactionsTopic"));

    stream.writeAsCsv(output);

    env.execute("Flink csv Writer");
  }
}
