package org.paysim.paysim.flink;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.paysim.paysim.base.Transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

public class TransactionsCollectionProducer {


  public void run(StreamExecutionEnvironment env, String brokers, String inputTopic, ArrayList<Transaction> transactions) throws Exception {


    Properties dataKafkaProps = new Properties();

//    String materialPasswd = readMaterialPassword();

    // Replace this the list of your brokers, even better if you make it configurable from the job arguments
    dataKafkaProps.setProperty("bootstrap.servers", brokers);

    // These settings are static and they don't need to be changed
//    dataKafkaProps.setProperty("security.protocol", "SSL");
//    dataKafkaProps.setProperty("ssl.truststore.location", "t_certificate");
//    dataKafkaProps.setProperty("ssl.truststore.password", materialPasswd);
//    dataKafkaProps.setProperty("ssl.keystore.location", "k_certificate");
//    dataKafkaProps.setProperty("ssl.keystore.password", materialPasswd);
//    dataKafkaProps.setProperty("ssl.key.password", materialPasswd);
//    dataKafkaProps.setProperty("ssl.endpoint.identification.algorithm", "");
    dataKafkaProps.setProperty("group.id", "consumer-group");


    DataStream<Transaction> stream = env.fromCollection(transactions);
//    DataStream<String> stream = env.addSource(            new SourceFunction<String>() {
//      volatile boolean running = true;
//
//      public void run(SourceFunction.SourceContext<String> sourceContext) throws Exception {
//        sourceContext.collect(transaction.toString());
////        while (running) {
////          sourceContext.collect(transaction.toString());
////          Thread.sleep(100);
////        }
//      }
//
//      public void cancel() {
//        running = false;
//      }
//    });


    FlinkKafkaProducer<Transaction> myProducer = new FlinkKafkaProducer<Transaction>(
      inputTopic,                 // target topic
      new TransactionSchema(),    // serialization schema
//      new SimpleStringSchema(),
      dataKafkaProps)             // producer config
      ;

    stream.addSink(myProducer);

  }

  private String readMaterialPassword() throws Exception {
    return FileUtils.readFileToString(new File("material_passwd"));
  }

}
