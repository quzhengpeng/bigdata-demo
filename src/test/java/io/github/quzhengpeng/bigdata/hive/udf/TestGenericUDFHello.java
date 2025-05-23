package io.github.quzhengpeng.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGenericUDFHello {

    @Test
    public void testHello() throws HiveException {
        GenericUDFHello udf = new GenericUDFHello();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector[] arguments = {valueOI0};
        udf.initialize(arguments);

        runAndVerify("A", "Hello, A", udf);
        runAndVerify("B", "Hello, B", udf);
    }

    private void runAndVerify(String str, String expResult, GenericUDF udf) throws HiveException {
        GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(str != null ? new Text(str) : null);
        GenericUDF.DeferredObject[] args = {valueObj0};
        Text output = (Text) udf.evaluate(args);
        assertEquals("hello() test ", expResult, output != null ? output.toString() : null);
    }
}
