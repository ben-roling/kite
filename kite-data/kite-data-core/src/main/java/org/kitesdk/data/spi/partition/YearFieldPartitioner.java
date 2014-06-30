/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.partition;

import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Range;
import java.util.Calendar;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.spi.Predicates;
import org.kitesdk.data.spi.Predicates.NamedPredicate;
import org.kitesdk.data.spi.Predicates.NamedRange;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
    justification="Implement if we intend to use in Serializable objects "
        + " (e.g., TreeMaps) and use java serialization.")
@Immutable
public class YearFieldPartitioner extends CalendarFieldPartitioner {

  public YearFieldPartitioner(String sourceName) {
    this(sourceName, null);
  }

  public YearFieldPartitioner(String sourceName, @Nullable String name) {
    super(sourceName, (name == null ? "year" : name), Calendar.YEAR, 5); // arbitrary number of partitions
  }

  @Override
  public NamedPredicate<Integer> project(NamedPredicate<Long> predicate) {
    // year is the only time field that can be projected
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.NamedIn) {
      return ((Predicates.NamedIn<Long>) predicate).transform(this);
    } else if (predicate instanceof Predicates.NamedRange) {
      return Predicates.transformClosed(
          Predicates.adjustClosed(
              (NamedRange<Long>) predicate, DiscreteDomains.longs()),
          this);
    } else {
      return null;
    }
  }

  @Override
  public NamedPredicate<Integer> projectStrict(NamedPredicate<Long> predicate) {
    if (predicate instanceof Predicates.Exists) {
      return Predicates.exists();
    } else if (predicate instanceof Predicates.NamedIn) {
      // not enough information to make a judgement on behalf of the
      // original predicate. the year may match when month does not
      return null;
    } else if (predicate instanceof Predicates.NamedRange) {
      //return Predicates.transformClosedConservative(
      //    (Range<Long>) predicate, this, DiscreteDomains.integers());
      NamedRange<Long> adjusted = Predicates.adjustClosed(
          (NamedRange<Long>) predicate, DiscreteDomains.longs());
      Range<Long> adjustedRange = adjusted.getRange();
      if (adjustedRange.hasLowerBound()) {
        long lower = adjustedRange.lowerEndpoint();
        int lowerImage = apply(lower);
        if (apply(lower - 1) == lowerImage) {
          // at least one excluded value maps to the same lower endpoint
          lowerImage += 1;
        }
        if (adjustedRange.hasUpperBound()) {
          long upper = adjustedRange.upperEndpoint();
          int upperImage = apply(upper);
          if (apply(upper + 1) == upperImage) {
            // at least one excluded value maps to the same upper endpoint
            upperImage -= 1;
          }
          if (lowerImage <= upperImage) {
            return Predicates.closed(lowerImage, upperImage);
          }
        } else {
          return Predicates.atLeast(lowerImage);
        }
      } else if (adjustedRange.hasUpperBound()) {
        long upper = adjustedRange.upperEndpoint();
        int upperImage = apply(upper);
        if (apply(upper + 1) == upperImage) {
          // at least one excluded value maps to the same upper endpoint
          upperImage -= 1;
        }
        return Predicates.atMost(upperImage);
      }
    }
    // could not produce a satisfying predicate
    return null;
  }
}
