package org.kitesdk.data.spi;

import junit.framework.Assert;
import org.junit.Test;

public class TestPredicateNames {
  @Test
  public void testRanges() {
    Assert.assertEquals("[2,inf)", Predicates.atLeast(2).getName());
    Assert.assertEquals("(2,inf)", Predicates.greaterThan(2).getName());
    Assert.assertEquals("(-inf,2]", Predicates.atMost(2).getName());
    Assert.assertEquals("(-inf,2)", Predicates.lessThan(2).getName());
    Assert.assertEquals("[2,10]", Predicates.closed(2, 10).getName());
    Assert.assertEquals("(2,10)", Predicates.open(2, 10).getName());
    Assert.assertEquals("(2,10]", Predicates.openClosed(2, 10).getName());
    Assert.assertEquals("[2,10)", Predicates.closedOpen(2, 10).getName());
    Assert.assertEquals("2", Predicates.singleton(2).getName());
  }
  
  @Test
  public void testIn() {
    Assert.assertEquals("in(2,10)", Predicates.in(2, 10).getName());
    Assert.assertEquals("2", Predicates.in(2).getName());
    Assert.assertEquals("in()", Predicates.in().getName());
  }
  
  @Test
  public void testExists() {
    Assert.assertEquals("exists()", Predicates.exists().getName());
  }
}
