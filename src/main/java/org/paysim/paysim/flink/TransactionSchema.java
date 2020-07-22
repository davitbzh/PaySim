package org.paysim.paysim.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.paysim.paysim.base.Transaction;

/**
 * Implements a SerializationSchema and DeserializationSchema for TransactionSchema for Kafka data sources and sinks.
 */
public class TransactionSchema implements DeserializationSchema<Transaction>, SerializationSchema<Transaction> {

  @Override
  public byte[] serialize(Transaction element) {
    return element.toString().getBytes();
  }

  @Override
  public Transaction deserialize(byte[] message) {
    return Transaction.fromString(new String(message));
  }

  @Override
  public boolean isEndOfStream(Transaction nextElement) {
    return false;
  }

  @Override
  public TypeInformation<Transaction> getProducedType() {
    return TypeExtractor.getForClass(Transaction.class);
  }

}