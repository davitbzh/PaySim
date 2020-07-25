package org.paysim.paysim.flink.sinks;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.paysim.paysim.base.Transaction;

public class TransactionParquetSynk {
//  public static void main(String[] args) {
//    ParameterTool tool = ParameterTool.fromArgs(args);
//
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//    env.getConfig().enableObjectReuse();
//    env.enableCheckpointing(10000);
//
//    String output = tool.get("output", OUTPUT);
//
////    https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/streamfile_sink.html
////    https://github.com/sjwiesman/flink-parquet-writer/blob/d753cdcfc17a5bf40dde2ab7d5013bf24ee4935b/flink-job/src/main/java/com/ververica/example/FlinkParquetWriter.java
////    https://www.alibabacloud.com/blog/using-hive-in-apache-flink-1-9_595678
////    https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/hive/
////    https://developpaper.com/analysis-of-flink-kafka-connector-and-exactly-once/
//
//    env.addSource(createSourceFunction(tool))
//      .name("kafka-source")
//      .addSink(StreamingFileSink
//        .forBulkFormat(new Path(output), ParquetAvroWriters.forSpecificRecord(Transaction.class))
//        .build())
//      .name("parquet-writer");
//
//    env.execute("Flink Parquet Writer");
//  }
}
