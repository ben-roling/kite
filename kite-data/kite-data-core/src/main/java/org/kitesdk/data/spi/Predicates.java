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

package org.kitesdk.data.spi;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import java.util.Set;
import javax.annotation.Nullable;

public abstract class Predicates {
  public static abstract class NamedPredicate<T> implements Predicate<T> {
    public abstract String getName();
    
    public String toString() {
      return Objects.toStringHelper(this).add("name", getName()).toString();
    }
  }
  
  public static class NamedRange<T extends Comparable<T>> extends NamedPredicate<T> {
    private final Range<T> range;
    
    public NamedRange(Range<T> range) {
      this.range = range;
    }
    
    public Range<T> getRange() {
      return range;
    }
    
    public NamedRange<T> intersection(NamedRange<T> other) {
      return new NamedRange<T>(getRange().intersection(other.getRange()));
    }
    
    @Override
    public String getName() {
      StringBuffer buffer = new StringBuffer();
      if (range.hasLowerBound()) {
        buffer.append(range.lowerBoundType() == BoundType.CLOSED ? "[" : "(");
        buffer.append(range.lowerEndpoint());
      }
      else {
        buffer.append("(-inf");
      }
      buffer.append(",");
      if (range.hasUpperBound()) {
        buffer.append(range.upperEndpoint());
        buffer.append(range.upperBoundType() == BoundType.CLOSED ? "]" : ")");
      }
      else {
        buffer.append("inf)");
      } 
      return buffer.toString();
    }
    
    @Override
    public boolean apply(T input) {
      return range.apply(input);
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      return Objects.equal(range, ((NamedRange) o).range);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(range);
    }
  }
  
  public static <T extends Comparable<T>> NamedRange<T> atLeast(T value) {
    return new NamedRange<T>(Ranges.atLeast(value));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> greaterThan(T value) {
    return new NamedRange<T>(Ranges.greaterThan(value));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> atMost(T value) {
    return new NamedRange<T>(Ranges.atMost(value));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> lessThan(T value) {
    return new NamedRange<T>(Ranges.lessThan(value));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> closed(T lower, T upper) {
    return new NamedRange<T>(Ranges.closed(lower, upper));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> open(T lower, T upper) {
    return new NamedRange<T>(Ranges.open(lower, upper));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> openClosed(T lower, T upper) {
    return new NamedRange<T>(Ranges.openClosed(lower, upper));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> closedOpen(T lower, T upper) {
    return new NamedRange<T>(Ranges.closedOpen(lower, upper));
  }
  
  public static <T extends Comparable<T>> NamedRange<T> singleton(T value) {
    return new NamedRange<T>(Ranges.singleton(value));
  }
  
  @SuppressWarnings("unchecked")
  public static <T> Exists<T> exists() {
    return (Exists<T>) Exists.INSTANCE;
  }

  public static <T> NamedIn<T> in(Set<T> set) {
    return new NamedIn<T>(set);
  }

  public static <T> NamedIn<T> in(T... set) {
    return new NamedIn<T>(set);
  }

  // This should be a method on Range, like In#transform.
  // Unfortunately, Range is final so we will probably need to re-implement it.
  public static <S extends Comparable<S>, T extends Comparable<T>>
  NamedRange<T> transformClosed(NamedRange<S> predicate, Function<? super S, T> function) {
    Range<S> range = predicate.getRange();
    if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        return Predicates.closed(
            function.apply(range.lowerEndpoint()),
            function.apply(range.upperEndpoint()));
      } else {
        return Predicates.atLeast(function.apply(range.lowerEndpoint()));
      }
    } else if (range.hasUpperBound()) {
      return Predicates.atMost(function.apply(range.upperEndpoint()));
    } else {
      return null;
    }
  }

  public static <T extends Comparable<T>>
  NamedRange<T> adjustClosed(NamedRange<T> predicate, DiscreteDomain<T> domain) {
    Range<T> range = predicate.getRange();
    // adjust to a closed range to avoid catching extra keys
    if (range.hasLowerBound()) {
      T lower = range.lowerEndpoint();
      if (BoundType.OPEN == range.lowerBoundType()) {
        lower = domain.next(lower);
      }
      if (range.hasUpperBound()) {
        T upper = range.upperEndpoint();
        if (BoundType.OPEN == range.upperBoundType()) {
          upper = domain.previous(upper);
        }
        return closed(lower, upper);
      } else {
        return atLeast(lower);
      }
    } else if (range.hasUpperBound()) {
      T upper = range.upperEndpoint();
      if (BoundType.OPEN == range.upperBoundType()) {
        upper = domain.previous(upper);
      }
      return atMost(upper);
    } else {
      throw new IllegalArgumentException("Invalid range: no endpoints");
    }
  }

  public static class Exists<T> extends NamedPredicate<T> {
    public static final Exists INSTANCE = new Exists();

    private Exists() {
    }
    
    @Override
    public boolean apply(@Nullable T value) {
      return (value != null);
    }

    @Override
    public String getName() {
      return "exists()";
    }
  }
  
  public static class NamedIn<T> extends NamedPredicate<T> {
    // ImmutableSet entries are non-null
    private final ImmutableSet<T> set;
    
    public NamedIn(Iterable<T> values) {
      set = ImmutableSet.copyOf(values);
      Preconditions.checkArgument(set.size() > 0, "No values to match");
    }
    
    public NamedIn(T... values) {
      set = ImmutableSet.copyOf(values);
    }
         
    @Override
    public boolean apply(@Nullable T test) {
      // Set#contains may throw NPE, depending on implementation
      return (test != null) && set.contains(test);
    }

    public NamedIn<T> filter(NamedPredicate<T> predicate) {
      try {
        return new NamedIn<T>(Iterables.filter(set, predicate));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Filter predicate produces empty set", e);
      }
    }
    
    public <V> NamedIn<V> transform(Function<? super T, V> function) {
      return new NamedIn<V>(Iterables.transform(set, function));
    }

    @Override
    public String getName() {
      return "in(" + Iterables.toString(set) + ")";
    }

    Set<T> getSet() {
      return set;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      return Objects.equal(set, ((NamedIn) o).set);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(set);
    }
  }
}
