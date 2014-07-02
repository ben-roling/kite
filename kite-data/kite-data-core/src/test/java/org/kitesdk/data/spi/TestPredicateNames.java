/*
 * Copyright 2014 Cloudera Inc.
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

import java.util.Map;
import java.util.Map.Entry;
import junit.framework.Assert;
import org.junit.Test;
import org.kitesdk.data.spi.Predicates.NamedPredicate;
import com.google.common.collect.Maps;

public class TestPredicateNames {
  @SuppressWarnings("rawtypes")
  @Test
  public void testRanges() {
    // TODO - (-inf, inf) ?
    
    Map<NamedPredicate, String> integerRanges = Maps.newHashMap();
    integerRanges.put(Predicates.atLeast(2), "[2,inf)");
    integerRanges.put(Predicates.greaterThan(2), "(2,inf)");
    integerRanges.put(Predicates.atMost(2), "(-inf,2]");
    integerRanges.put(Predicates.lessThan(2), "(-inf,2)");
    integerRanges.put(Predicates.closed(2, 10), "[2,10]");
    integerRanges.put(Predicates.open(2, 10), "(2,10)");
    integerRanges.put(Predicates.openClosed(2, 10), "(2,10]");
    integerRanges.put(Predicates.closedOpen(2, 10), "[2,10)");
    
    assertNames(integerRanges, Integer.class);
    
    Map<NamedPredicate, String> stringRanges = Maps.newHashMap();
    stringRanges.put(Predicates.atLeast("Smith, John"), "[Smith%2C John,inf)");
    stringRanges.put(Predicates.greaterThan("Smith, John"), "(Smith%2C John,inf)");
    stringRanges.put(Predicates.atMost("Smith, John"), "(-inf,Smith%2C John]");
    stringRanges.put(Predicates.lessThan("Smith, John"), "(-inf,Smith%2C John)");
    stringRanges.put(Predicates.closed("Smith, John", "Tanner, Ted"), "[Smith%2C John,Tanner%2C Ted]");
    stringRanges.put(Predicates.open("Smith, John", "Tanner, Ted"), "(Smith%2C John,Tanner%2C Ted)");
    stringRanges.put(Predicates.openClosed("Smith, John", "Tanner, Ted"), "(Smith%2C John,Tanner%2C Ted]");
    stringRanges.put(Predicates.closedOpen("Smith, John", "Tanner, Ted"), "[Smith%2C John,Tanner%2C Ted)");
    
    assertNames(stringRanges, String.class);
    
    Assert.assertEquals("2", Predicates.singleton(2).getName());
    Assert.assertEquals("Smith, John", Predicates.singleton("Smith, John").getName());
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void testIn() {
    Map<NamedPredicate, String> integerTests = Maps.newHashMap();
    integerTests.put(Predicates.in(2, 10), "in(2,10)");
    integerTests.put(Predicates.in(2), "2");
    
    assertNames(integerTests, Integer.class);
    
    Map<NamedPredicate, String> stringTests = Maps.newHashMap();
    integerTests.put(Predicates.in("Smith, John", "Tanner, Ted"), "in(Smith%2C John,Tanner%2C Ted)");
    integerTests.put(Predicates.in("Smith, John"), "Smith, John");
    
    assertNames(stringTests, String.class);
  }

  @Test
  public void testExists() {
    Assert.assertEquals("exists()", Predicates.exists().getName());
    Assert.assertEquals(Predicates.exists(), Predicates.fromName("exists()", Integer.class));
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void assertNames(Map<NamedPredicate, String> expectedNameByPredicate, Class clazz) {
    for (Entry<NamedPredicate, String> entry : expectedNameByPredicate.entrySet()) {
      NamedPredicate predicate = entry.getKey();
      String expectedName = entry.getValue();
      Assert.assertEquals(expectedName, predicate.getName());
      Assert.assertEquals(predicate, Predicates.fromName(expectedName, clazz));
    }
  }
}
