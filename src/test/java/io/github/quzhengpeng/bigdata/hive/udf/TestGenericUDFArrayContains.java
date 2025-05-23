package io.github.quzhengpeng.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestGenericUDFArrayContains {

    @Test
    public void testArrayContains() throws HiveException {
        GenericUDFArrayContains udf = new GenericUDFArrayContains();
        ObjectInspector valueOI0 = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
                TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo));
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(new ObjectInspector[]{valueOI0, valueOI1});

        runAndVerify(Arrays.asList("A", "B", "C"), "B", true, udf);
        runAndVerify(Arrays.asList("A", "B", "C"), "C", true, udf);
        runAndVerify(Arrays.asList("A", "B", "C"), "D", false, udf);
    }

    private void runAndVerify(List<String> list, String str, Boolean expResult, GenericUDF udf) throws HiveException {
        GenericUDF.DeferredObject valueObj0 = new GenericUDF.DeferredJavaObject(list != null ? new ArrayList<String>(list) : null);
        GenericUDF.DeferredObject valueObj1 = new GenericUDF.DeferredJavaObject(str != null ? new Text(str) : null);
        GenericUDF.DeferredObject[] args = {valueObj0, valueObj1};
        BooleanWritable output = (BooleanWritable) udf.evaluate(args);
        assertEquals("array_contains() test ", output.get(), expResult);
    }
}
