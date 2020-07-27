package org.paysim.paysim.flink.sinks;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.paysim.paysim.base.Transaction;
import org.paysim.paysim.flink.consumers.TransactionConsumer;

import java.time.ZoneId;

public class TransactionBucketingSink {

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.enableCheckpointing(1000);

    String output = params.get("file_output_path");

    DataStream<Transaction> stream = TransactionConsumer
      .run(env, params.get("brokers", "localhost:9092"), params.get("topic", "transactionsTopic"));


//    https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/filesystem_sink.html
    BucketingSink<Transaction> sink = new BucketingSink<Transaction>(output);
//    sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")));
////    sink.setWriter(new SequenceFileWriter<Transaction>());
//    sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
//    sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

    stream.addSink(sink);

    env.execute("Flink csv Writer");
  }
}
