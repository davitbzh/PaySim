package org.paysim.paysim.flink;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.paysim.paysim.base.Transaction;

import java.io.File;
import java.util.Properties;

public class TransactionConsumer {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);
    TransactionConsumer.run(params.get("brokers", "localhost:9092"), params.get("topic", "transactionsTopic"));
  }

  private static void run(String brokers, String inputTopic) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties dataKafkaProps = new Properties();

    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//    String materialPasswd = readMaterialPassword();
    // Replace this the list of your brokers, even better if you make it configurable from the job arguments
    dataKafkaProps.setProperty("bootstrap.servers", brokers);

//    // These settings are static and they don't need to be changed
//    dataKafkaProps.setProperty("security.protocol", "SSL");
//    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
//    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
//    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
//    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
//    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
//    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");
    dataKafkaProps.setProperty("group.id", "consumer-group");


    FlinkKafkaConsumer<Transaction> myConsumer = new FlinkKafkaConsumer<Transaction>(
      inputTopic,                 // target topic
      new TransactionSchema(),    // serialization schema
      dataKafkaProps)             // producer config
      ;

    DataStream<Transaction> messageStream =
      env.addSource(myConsumer);

    messageStream.print();

    env.execute();

  }

  private static String readMaterialPassword() throws Exception {
    return FileUtils.readFileToString(new File("material_passwd"));
  }

}
