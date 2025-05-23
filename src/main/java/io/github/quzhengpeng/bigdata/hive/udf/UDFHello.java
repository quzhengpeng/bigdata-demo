package io.github.quzhengpeng.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/*
  DELETE jar hdfs:///user/hive/lib/bigdata-demo-1.0-SNAPSHOT.jar;
  ADD    jar hdfs:///user/hive/lib/bigdata-demo-1.0-SNAPSHOT.jar;
  CREATE TEMPORARY FUNCTION hello AS 'io.github.quzhengpeng.bigdata.hive.udf.UDFHello';
  desc function extended hello;
*/

@Description(name = "hello",
        value = "_FUNC_(str) - Add string Hello before input str",
        extended = "Example:\n  > SELECT _FUNC_('A') FROM src LIMIT 1;\n  hello, A"
)

public class UDFHello extends UDF {
    public String evaluate(String str) {
        try {
            return "Hello, " + str;
        } catch (Exception e) {
            e.fillInStackTrace();
            return "ERROR";
        }
    }
}
