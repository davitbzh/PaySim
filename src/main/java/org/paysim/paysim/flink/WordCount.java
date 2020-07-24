package org.paysim.paysim.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount{

  public static void main( String[] args ) throws Exception{

    // set up the execution environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // input data
    // you can also use env.readTextFile(...) to get words
    DataSet<String> text = env.fromElements(
      "To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,"
    );

    DataSet<Tuple2<String, Integer>> counts =
      // split up the lines in pairs (2-tuples) containing: (word,1)
      text.flatMap( new LineSplitter() )
        // group by the tuple field "0" and sum up tuple field "1"
        .groupBy( 0 )
        .aggregate( Aggregations.SUM, 1 );

    // emit result
    counts.print();
  }

  public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap( String value, Collector<Tuple2<String, Integer>> out ){
      // normalize and split the line into words
      String[] tokens = value.toLowerCase().split( "\\W+" );

      // emit the pairs
      for( String token : tokens ){
        if( token.length() > 0 ){
          out.collect( new Tuple2<String, Integer>( token, 1 ) );
        }
      }
    }
  }

}

