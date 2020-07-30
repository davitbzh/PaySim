package org.paysim.paysim.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.paysim.paysim.PaySim;
import org.paysim.paysim.base.Transaction;

import org.apache.spark.sql.avro.package$;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.avro.SchemaConverters;

public class StructuredStreamingKafka {

  public void run(Dataset<Row> df, String brokers, String inputTopic) throws Exception {

//// `from_avro` requires Avro schema in JSON string format.
    String jsonFormatSchema = org.paysim.paysim.avro.generated.Transaction.getClassSchema().toString();


    String materialPasswd = readMaterialPassword();


    df.select(
      col("step"),
      col("action"),
      col("amount"),
      col("nameOrig"),
      col("oldBalanceOrig"),
      col("newBalanceOrig"),
      col("nameDest"),
      col("oldBalanceDest"),
      col("newBalanceDest"),
      col("failedTransaction"),
      col("fraud"),
      col("flaggedFraud"),
      col("unauthorizedOverdraft"))
      .select(package$.MODULE$.from_avro(df.col("*"), jsonFormatSchema).as("value"))
      .writeStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", inputTopic)

      .option("kafka.security.protocol", "SSL")
      .option("kafka.ssl.truststore.location", "t_certificate")
      .option("kafka.ssl.truststore.password", materialPasswd)
      .option("kafka.ssl.keystore.location", "k_certificate")
      .option("kafka.ssl.keystore.password", materialPasswd)
      .option("kafka.ssl.key.password", materialPasswd)
      .option("kafka.ssl.endpoint.identification.algorithm", "")
      .start();

  }

  private String readMaterialPassword() throws Exception {
    return FileUtils.readFileToString(new File("material_passwd"));
  }

}

//.map(x -> {
//  org.paysim.paysim.avro.generated.Transaction avroTransactions = new org.paysim.paysim.avro.generated.Transaction();
//  avroTransactions.setStep(x.getStep());
//  avroTransactions.setAction(x.getAction());
//  avroTransactions.setAmount(x.getAmount());
//  avroTransactions.setNameOrig(x.getNameOrig());
//  avroTransactions.setOldBalanceOrig(x.getOldBalanceOrig());
//  avroTransactions.setNewBalanceOrig(x.getNewBalanceOrig());
//  avroTransactions.setNameDest(x.getNameDest());
//  avroTransactions.setOldBalanceDest(x.getOldBalanceDest());
//  avroTransactions.setNewBalanceDest(x.getNewBalanceDest());
//
//  avroTransactions.setIsFailedTransaction(x.isFailedTransaction());
//  avroTransactions.setIsFraud(x.isFraud());
//  avroTransactions.setIsFlaggedFraud(x.isFlaggedFraud());
//  avroTransactions.setIsUnauthorizedOverdraft(x.isUnauthorizedOverdraft());
//
//  return avroTransactions;
//
//  });
//
//  transactionRDD.take(10);
//
//  StructType structType = new StructType();
//  for (Schema.Field field : org.paysim.paysim.avro.generated.Transaction.SCHEMA$.getFields()) {
//  structType.add(field.name(), SchemaConverters.toSqlType(field.schema()).dataType());
//  }
//
//  StructType requiredType = (StructType) SchemaConverters.toSqlType(org.paysim.paysim.avro.generated.Transaction.getClassSchema()).dataType();
