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
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

public abstract class Predicates {
  private static final String ESCAPED_COMMA = "%2C";
  private static final String ESCAPED_COMMA_PATTERN = Pattern.quote(ESCAPED_COMMA);
  
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
    
    private static String escapedValue(String value) {
      return value.replaceAll(",", ESCAPED_COMMA);
    }
    
    private static String unescapedValue(String value) {
      return value.replaceAll(ESCAPED_COMMA_PATTERN, ",");
    }
    
    @Override
    public String getName() {
      if (range.hasLowerBound() && range.hasUpperBound()
          && range.lowerEndpoint().equals(range.upperEndpoint())) {
        return range.lowerEndpoint().toString();
      }
      StringBuffer buffer = new StringBuffer();
      if (range.hasLowerBound()) {
        buffer.append(range.lowerBoundType() == BoundType.CLOSED ? "[" : "(");
        buffer.append(escapedValue(Conversions.makeString(range.lowerEndpoint())));
      }
      else {
        buffer.append("(-inf");
      }
      buffer.append(",");
      if (range.hasUpperBound()) {
        buffer.append(escapedValue(Conversions.makeString(range.upperEndpoint())));
        buffer.append(range.upperBoundType() == BoundType.CLOSED ? "]" : ")");
      }
      else {
        buffer.append("inf)");
      } 
      return buffer.toString();
    }
    
    private static <T extends Comparable<T>> NamedPredicate<T> fromName(
        String name, Class<T> clazz) {
      String [] parts = name.split(",");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Unsupported name: " + name);
      }
      if (parts[0].length() < 2 || parts[1].length() < 2) {
        throw new IllegalArgumentException("Unsupported name: " + name);
      }
      String lowerBound = parts[0];
      BoundType lowerBoundType = null;
      if (lowerBound.startsWith("(")) {
        lowerBoundType = BoundType.OPEN;
      }
      else if (lowerBound.startsWith("[")) {
        lowerBoundType = BoundType.CLOSED;
      }
      lowerBound = lowerBound.substring(1);
      
      String upperBound = parts[1];
      BoundType upperBoundType = null;
      if (upperBound.endsWith(")")) {
        upperBoundType = BoundType.OPEN;
      }
      else if (upperBound.endsWith("]")) {
        upperBoundType = BoundType.CLOSED;
      }
      upperBound = upperBound.substring(0, upperBound.length()-1);
      
      if ("-inf".equals(lowerBound)) {
        if (upperBoundType == BoundType.CLOSED) {
          return atMost(Conversions.convert(unescapedValue(upperBound), clazz));
        }
        else if (upperBoundType == BoundType.OPEN) {
          return lessThan(Conversions.convert(unescapedValue(upperBound), clazz));
        }
        else {
          throw new IllegalArgumentException("Unsupported name: " + name);
        }
      } else if ("inf".equals(upperBound)) {
        if (lowerBoundType == BoundType.CLOSED) {
          return atLeast(Conversions.convert(unescapedValue(lowerBound), clazz));
        } else if (lowerBoundType == BoundType.OPEN) {
          return greaterThan(Conversions.convert(unescapedValue(lowerBound), clazz));
        } else {
          throw new IllegalArgumentException("Unsupported name: " + name);
        }
      } else {
        return range(Conversions.convert(unescapedValue(lowerBound), clazz), lowerBoundType,
            Conversions.convert(unescapedValue(upperBound), clazz), upperBoundType);
      }
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
  
  private static <T extends Comparable<T>> NamedRange<T> range(T lowerBound,
      BoundType lowerBoundType, T upperBound, BoundType upperBoundType) {
    return new NamedRange<T>(Ranges.range(lowerBound, lowerBoundType,
        upperBound, upperBoundType));
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
      Preconditions.checkArgument(set.size() > 0, "No values to match");
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
      StringBuffer buffer = new StringBuffer();
      buffer.append("in(");
      int numValues = 0;
      for (T value : set) {
        if (numValues > 0) {
          buffer.append(",");
        }
        buffer.append(escapedValue(Conversions.makeString(value)));
        numValues++;
      }
      buffer.append(")");
      if (numValues == 1) {
        return escapedValue(Conversions.makeString(Iterables.getOnlyElement(set)));
      }
      return buffer.toString();
    }
    
    private static String escapedValue(String value) {
      return value.replaceAll(",", ESCAPED_COMMA);
    }
    
    private static String unescapedValue(String value) {
      return value.replaceAll(ESCAPED_COMMA_PATTERN, ",");
    }
    
    static <T extends Comparable<T>> NamedPredicate<T> fromName(
        String name, Class<T> clazz) {
      if (!name.endsWith(")")) {
        throw new IllegalArgumentException("Unsupported name: " + name);
      }
      StringTokenizer tokenizer = new StringTokenizer(name.substring(3, name.length()-1), ",");
      HashSet<T> elements = Sets.newHashSet();
      while(tokenizer.hasMoreElements()) {
        elements.add(Conversions.convert(unescapedValue(tokenizer.nextToken()), clazz));
      }
      return in(elements);
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
  
  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> NamedPredicate<T> fromName(String name, Class<T> clazz) {
    Preconditions.checkNotNull(name, "name:null");
    if (name.startsWith("in(") && name.endsWith(")")) {
      return NamedIn.fromName(name, clazz);
    } else if ("exists()".equals(name)) {
      return exists();
    } else if ((name.startsWith("[") || name.startsWith("("))
        && (name.endsWith("]") || name.endsWith(")"))) {
      return NamedRange.fromName(name, clazz);
    }
    
    // must be a single value for an equality criteria
    return in(Conversions.convert(name, clazz));
  }
}
