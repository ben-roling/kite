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
  static abstract class NamedPredicate<T> implements Predicate<T> {
    private Predicate<T> predicate;
    
    public NamedPredicate(Predicate<T> predicate) {
      this.predicate = predicate;
    }
    
    public abstract String getName();
    
    @Override
    public boolean apply(T input) {
      return predicate.apply(input);
    }  
  }
  
  static class NamedRangePredicate<T extends Comparable<T>> extends NamedPredicate<T> {
    private final Range<T> range;
    
    public NamedRangePredicate(Range<T> range) {
      super(range);
      this.range = range;
    }
    
    public Range<T> getPredicate() {
      return range;
    }
    
    public NamedRangePredicate<T> intersection(NamedRangePredicate<T> other) {
      return new NamedRangePredicate<T>(getPredicate().intersection(other.getPredicate()));
    }
    
    @Override
    public String getName() {
      Range<T> predicate = getPredicate();
      return (predicate.lowerBoundType() == BoundType.CLOSED ? "[" : "(")
        + (predicate.hasLowerBound() ? predicate.lowerEndpoint() : "-inf")
        + ","
        + (predicate.hasUpperBound() ? predicate.upperEndpoint() : "inf")
        + (predicate.upperBoundType() == BoundType.CLOSED ? "]" : ")");
    }
  }
  
  static <T extends Comparable<T>> NamedRangePredicate<T> atLeast(T value) {
    return new NamedRangePredicate<T>(Ranges.atLeast(value));
  }
  
  static <T extends Comparable<T>> NamedRangePredicate<T> greaterThan(T value) {
    return new NamedRangePredicate<T>(Ranges.greaterThan(value));
  }
  
  static <T extends Comparable<T>> NamedRangePredicate<T> atMost(T value) {
    return new NamedRangePredicate<T>(Ranges.atMost(value));
  }
  
  static <T extends Comparable<T>> NamedRangePredicate<T> lessThan(T value) {
    return new NamedRangePredicate<T>(Ranges.lessThan(value));
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
  public static <S extends Comparable, T extends Comparable>
  Range<T> transformClosed(Range<S> range, Function<? super S, T> function) {
    if (range.hasLowerBound()) {
      if (range.hasUpperBound()) {
        return Ranges.closed(
            function.apply(range.lowerEndpoint()),
            function.apply(range.upperEndpoint()));
      } else {
        return Ranges.atLeast(function.apply(range.lowerEndpoint()));
      }
    } else if (range.hasUpperBound()) {
      return Ranges.atMost(function.apply(range.upperEndpoint()));
    } else {
      return null;
    }
  }

  public static <T extends Comparable>
  Range<T> adjustClosed(Range<T> range, DiscreteDomain<T> domain) {
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
        return Ranges.closed(lower, upper);
      } else {
        return Ranges.atLeast(lower);
      }
    } else if (range.hasUpperBound()) {
      T upper = range.upperEndpoint();
      if (BoundType.OPEN == range.upperBoundType()) {
        upper = domain.previous(upper);
      }
      return Ranges.atMost(upper);
    } else {
      throw new IllegalArgumentException("Invalid range: no endpoints");
    }
  }

  public static class Exists<T> extends NamedPredicate<T> {
    public static final Exists INSTANCE = new Exists();

    private Exists() {
      super(new Predicate<T>() {

        @Override
        public boolean apply(@Nullable T value) {
          return (value != null);
        }});
    }

    @Override
    public String getName() {
      return "exists()";
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).toString();
    }

  }
  
  public static class NamedIn<T> extends NamedPredicate<T> {
    private final Set<T> set;
    
    public NamedIn(Iterable<T> values) {
      super(new In<T>(values));
      set = ImmutableSet.copyOf(values);
    }
    
    public NamedIn(T... values) {
      super(new In<T>(values));
      set = ImmutableSet.copyOf(values);
    }

    public NamedPredicate<T> filter(NamedPredicate<T> additional) {
      return new NamedIn<T>(Iterables.filter(set, additional));
    }

    @Override
    public String getName() {
      return "in(" + set + ")";
    }

    public Set<T> getSet() {
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

  private static class In<T> implements Predicate<T> {
    // ImmutableSet entries are non-null
    private final ImmutableSet<T> set;

    public In(Iterable<T> values) {
      this.set = ImmutableSet.copyOf(values);
      Preconditions.checkArgument(set.size() > 0, "No values to match");
    }

    public In(T... values) {
      this.set = ImmutableSet.copyOf(values);
    }

    @Override
    public boolean apply(@Nullable T test) {
      // Set#contains may throw NPE, depending on implementation
      return (test != null) && set.contains(test);
    }

    public In<T> filter(Predicate<? super T> predicate) {
      try {
        return new In<T>(Iterables.filter(set, predicate));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Filter predicate produces empty set", e);
      }
    }

    public <V> In<V> transform(Function<? super T, V> function) {
      return new In<V>(Iterables.transform(set, function));
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

      return Objects.equal(set, ((In) o).set);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(set);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("set", set).toString();
    }
  }
}
