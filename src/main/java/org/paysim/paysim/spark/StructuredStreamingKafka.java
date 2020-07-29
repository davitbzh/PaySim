package org.paysim.paysim.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.paysim.paysim.PaySim;
import org.paysim.paysim.base.Transaction;

public class StructuredStreamingKafka {

  public static void main(String[] args) throws Exception {
    // configure spark
    SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD").setMaster("local[2]");
    // start a spark context
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(sc);



    // sample collection
    List<Integer> collection = Arrays.asList(1, 2); //, 3, 4, 5, 6, 7, 8, 9, 10

    // parallelize the collection to two partitions
    JavaRDD<Integer> rdd = sc.parallelize(collection);

    System.out.println("Number of partitions : "+rdd.getNumPartitions());


    System.out.println("AAA : "+ Thread.currentThread().getContextClassLoader().getResource("PaySim.properties"));

    JavaRDD<Object> transactionRDD = rdd.flatMap(x -> new PaySim().run().iterator()).map(x -> {
      return x;
    });

//    rdd.flatMap(x -> new PaySim().run().iterator()).map(
//      new Function<ArrayList<Transaction>, Transaction>() {
//        @Override
//        public Transaction call(ArrayList<Transaction> transactions) {
//          // <name><,><age><,><country>
//          return transactions;
//        }
//      });

    sqlContext.createDataFrame(transactionRDD, Transaction.class).show();

//    Dataset<Person> personDS =  sqlContext.createDataset(transactionRDD, Encoders.bean(Transaction.class));



//    rdd.foreach(new VoidFunction<Integer>(){
//
//      public void call(Integer number) throws Exception {
//        new PaySim().run();
//      }
//    });


//    rdd.foreach(new VoidFunction<Integer>(){
//      public void call(Integer number) {
//        System.out.println(number);
//      }
//    });

  }

}

