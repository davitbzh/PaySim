package org.paysim.paysim.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.paysim.paysim.base.Transaction;

public class TransactionSource implements SourceFunction<Transaction> {

  private final Transaction transaction;
  private volatile boolean isRunning = true;

  public TransactionSource(Transaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public void run(SourceContext<Transaction> sourceContext) throws Exception {

    sourceContext.collect(this.transaction);

//    while (isRunning) {
//      Thread.sleep(100);
//    }

  }

  @Override
  public void cancel() {
    isRunning = false;

  }

}