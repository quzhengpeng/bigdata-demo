package io.github.quzhengpeng.bigdata.hive.udf;

import org.junit.Assert;
import org.junit.Test;

public class TestUDFHello {
    private final UDFHello hello = new UDFHello();

    @Test
    public void testEvaluate() {
        String message = "Hello, A";
        Assert.assertEquals(message, hello.evaluate("A"));
    }
}
