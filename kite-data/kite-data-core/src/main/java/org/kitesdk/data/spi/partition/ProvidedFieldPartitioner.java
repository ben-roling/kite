package org.kitesdk.data.spi.partition;

import org.kitesdk.data.spi.FieldPartitioner;

import com.google.common.base.Predicate;

public class ProvidedFieldPartitioner<S extends Comparable<S>> extends
    FieldPartitioner<S, S> {
  public ProvidedFieldPartitioner(String name, Class<S> clazz) {
    super(null, name, null, clazz);
  }

  @Override
  public int compare(S o1, S o2) {
    return o1.compareTo(o2);
  }

  @Override
  public S apply(S value) {
    return value;
  }

  @Override
  public Predicate<S> project(Predicate<S> predicate) {
    return predicate;
  }

  @Override
  public Predicate<S> projectStrict(Predicate<S> predicate) {
    return predicate;
  }

}
