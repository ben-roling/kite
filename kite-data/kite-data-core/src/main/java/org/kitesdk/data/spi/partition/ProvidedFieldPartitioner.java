/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.partition;

import org.kitesdk.data.spi.FieldPartitioner;

import com.google.common.base.Predicate;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
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
