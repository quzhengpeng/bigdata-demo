package io.github.quzhengpeng.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/*
  DELETE jar hdfs:///user/hive/lib/bigdata-demo-1.0-SNAPSHOT.jar;
  ADD    jar hdfs:///user/hive/lib/bigdata-demo-1.0-SNAPSHOT.jar;
  CREATE TEMPORARY FUNCTION hello AS 'io.github.quzhengpeng.bigdata.hive.udf.GenericUDFHello';
  desc function extended hello;
*/

@Description(name = "hello",
        value = "_FUNC_(str) - Add string Hello before input str",
        extended = "Example:\n  > SELECT _FUNC_('A') FROM src LIMIT 1;\n  hello, A"
)

public class GenericUDFHello extends GenericUDF {
    /**
     * Example for GenericUDFLength code.
     * <a href="https://github.com/apache/hive/blob/7d69a8ce8cebf9a6d255d5aa998584e4e183085c/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFLength.java">...</a>
     */

    private final Text result = new Text();
    private transient PrimitiveObjectInspector argumentOI;
    private transient PrimitiveObjectInspectorConverter.StringConverter stringConverter;
    private transient PrimitiveObjectInspectorConverter.BinaryConverter binaryConverter;
    private transient boolean isInputString;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    "hello requires 1 argument, got " + arguments.length);
        }

        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException(
                    "hello only takes primitive types, got " + argumentOI.getTypeName());
        }
        argumentOI = (PrimitiveObjectInspector) arguments[0];

        PrimitiveObjectInspector.PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
        ObjectInspector outputOI = null;
        switch (inputType) {
            case CHAR:
            case VARCHAR:
            case STRING:
                isInputString = true;
                stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI);
                break;

            case BINARY:
                isInputString = false;
                binaryConverter = new PrimitiveObjectInspectorConverter.BinaryConverter(argumentOI,
                        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
                break;

            default:
                throw new UDFArgumentException(
                        " hello() only takes STRING/CHAR/VARCHAR/BINARY types as first argument, got "
                                + inputType);
        }

        outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        return outputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        byte[] data = null;
        if (isInputString) {
            String val = null;
            if (arguments[0] != null) {
                val = (String) stringConverter.convert(arguments[0].get());
            }
            if (val == null) {
                return null;
            }
            result.set("Hello, " + val);
        } else {
            BytesWritable val = null;
            if (arguments[0] != null) {
                val = (BytesWritable) binaryConverter.convert(arguments[0].get());
            }
            if (val == null) {
                return null;
            }
            result.set("Hello, " + val);
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("hello", children);
    }
}
