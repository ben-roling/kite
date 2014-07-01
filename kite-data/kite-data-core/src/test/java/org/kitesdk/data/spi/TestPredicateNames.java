package org.kitesdk.data.spi;

import junit.framework.Assert;
import org.junit.Test;

public class TestPredicateNames {
  @Test
  public void testRanges() {
    // TODO - (-inf, inf) ?
    
    Assert.assertEquals("[2,inf)", Predicates.atLeast(2).getName());
    Assert.assertEquals(Predicates.atLeast(2), Predicates.fromName("[2,inf)", Integer.class));
    
    Assert.assertEquals("(2,inf)", Predicates.greaterThan(2).getName());
    Assert.assertEquals(Predicates.greaterThan(2), Predicates.fromName("(2,inf)", Integer.class));
    
    Assert.assertEquals("(-inf,2]", Predicates.atMost(2).getName());
    Assert.assertEquals(Predicates.atMost(2), Predicates.fromName("(-inf,2]", Integer.class));
    
    Assert.assertEquals("(-inf,2)", Predicates.lessThan(2).getName());
    Assert.assertEquals(Predicates.lessThan(2), Predicates.fromName("(-inf,2)", Integer.class));
    
    Assert.assertEquals("[2,10]", Predicates.closed(2, 10).getName());
    Assert.assertEquals(Predicates.closed(2, 10), Predicates.fromName("[2,10]", Integer.class));
    
    Assert.assertEquals("(2,10)", Predicates.open(2, 10).getName());
    Assert.assertEquals(Predicates.open(2, 10), Predicates.fromName("(2,10)", Integer.class));
    
    Assert.assertEquals("(2,10]", Predicates.openClosed(2, 10).getName());
    Assert.assertEquals(Predicates.openClosed(2, 10), Predicates.fromName("(2,10]", Integer.class));
    
    Assert.assertEquals("[2,10)", Predicates.closedOpen(2, 10).getName());
    Assert.assertEquals(Predicates.closedOpen(2, 10), Predicates.fromName("[2,10)", Integer.class));
    
    Assert.assertEquals("2", Predicates.singleton(2).getName());
  }
  
  @Test
  public void testIn() {
    Assert.assertEquals("in(2,10)", Predicates.in(2, 10).getName());
    Assert.assertEquals(Predicates.in(2, 10), Predicates.fromName("in(2,10)", Integer.class));
    
    Assert.assertEquals("2", Predicates.in(2).getName());
    Assert.assertEquals(Predicates.in(2), Predicates.fromName("2", Integer.class));
  }
  
  @Test
  public void testExists() {
    Assert.assertEquals("exists()", Predicates.exists().getName());
    Assert.assertEquals(Predicates.exists(), Predicates.fromName("exists()", Integer.class));
  }
}
