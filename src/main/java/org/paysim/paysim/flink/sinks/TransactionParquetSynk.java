package org.paysim.paysim.flink.sinks;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.paysim.paysim.flink.consumers.TransactionConsumer;

import java.io.File;

public class TransactionParquetSynk {

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().enableObjectReuse();
    env.enableCheckpointing(1000);

    String output = params.get("parquet_output_path");

//    https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/streamfile_sink.html
//    https://github.com/sjwiesman/flink-parquet-writer/blob/d753cdcfc17a5bf40dde2ab7d5013bf24ee4935b/flink-job/src/main/java/com/ververica/example/FlinkParquetWriter.java
//    https://www.alibabacloud.com/blog/using-hive-in-apache-flink-1-9_595678
//    https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/hive/
//    https://developpaper.com/analysis-of-flink-kafka-connector-and-exactly-once/

    DataStream<org.paysim.paysim.avro.generated.Transaction> stream = TransactionConsumer
      .run(env, params.get("brokers", "localhost:9092"), params.get("topic", "transactionsTopic"))
      .map(new MapFunction<org.paysim.paysim.base.Transaction, org.paysim.paysim.avro.generated.Transaction>() {
        @Override
        public org.paysim.paysim.avro.generated.Transaction map(org.paysim.paysim.base.Transaction transaction) throws Exception {
          org.paysim.paysim.avro.generated.Transaction result = new org.paysim.paysim.avro.generated.Transaction();
          result.setStep(transaction.getStep());
          result.setAction(transaction.getAction());
          result.setAmount(transaction.getAmount());
          result.setNameOrig(transaction.getNameOrig());
          result.setOldBalanceOrig(transaction.getOldBalanceOrig());
          result.setNewBalanceOrig(transaction.getNewBalanceOrig());
          result.setNameDest(transaction.getNameDest());
          result.setOldBalanceDest(transaction.getOldBalanceDest());
          result.setNewBalanceDest(transaction.getNewBalanceDest());
          result.setIsFraud(transaction.isFraud());
          result.setIsFlaggedFraud(transaction.isFlaggedFraud());
          result.setIsUnauthorizedOverdraft(transaction.isUnauthorizedOverdraft());

          return result;
        }
      });

//    final Schema schema = org.paysim.paysim.avro.generated.Transaction.getClassSchema();
//    stream.addSink(
//      StreamingFileSink.forBulkFormat(
//        new Path(output),
//        ParquetAvroWriters.forGenericRecord(schema))
//        .build());

    stream.addSink(
      StreamingFileSink.forBulkFormat(
        new Path(output),
        ParquetAvroWriters.forReflectRecord(org.paysim.paysim.avro.generated.Transaction.class))
        .build());

//    stream.addSink(StreamingFileSink
//      .forBulkFormat(new Path(output), ParquetAvroWriters.forSpecificRecord(org.paysim.paysim.avro.generated.Transaction.class))
//      .build())
//      .name("parquet-writer");


    env.execute("Flink Parquet Writer");
  }
}
