package org.paysim.paysim.flink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.paysim.paysim.base.Transaction;

import org.apache.commons.io.FileUtils;

import java.util.Properties;
import java.io.File;
// https://www.alibabacloud.com/blog/using-flink-connectors-correctly_595679
public class TransactionProducer {

  public void run(String brokers, String inputTopic, Transaction transaction) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    Properties dataKafkaProps = new Properties();

    String materialPasswd = readMaterialPassword();
    // Replace this the list of your brokers, even better if you make it configurable from the job arguments
    dataKafkaProps.setProperty("bootstrap.servers", brokers);

    // These settings are static and they don't need to be changed
    dataKafkaProps.setProperty("security.protocol", "SSL");
    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");

    DataStream<Transaction> stream = env.addSource(new TransactionSource(transaction));

    Properties properties = new Properties();

    FlinkKafkaProducer<Transaction> myProducer = new FlinkKafkaProducer<Transaction>(
      inputTopic,                 // target topic
      new TransactionSchema(),    // serialization schema
      properties)                 // producer config
      ;

    stream.addSink(myProducer);

  }

  private String readMaterialPassword() throws Exception {
    return FileUtils.readFileToString(new File("material_passwd"));
  }

}
