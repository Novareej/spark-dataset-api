
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import java.util.Collections;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.spark_partition_id;

public class AppDataSet {
    public static void main(String[] args) {
        SparkSession sp = SparkSession
                .builder()
                .appName("Spark SQL TP")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = sp.read().option("multiline",true).json("Employes.json");
        Dataset<Row> rsp =  df.select(col("id"),col("name"),col("phone"),col("salary"),
                col("age"),col("departement"));

        //Dataset typed
        Dataset<Employe> df2 = rsp.as(Encoders.bean(Employe.class));
        df2.show();

        df2.filter(df2.col("age").between(30,35)).show();
        df2.groupBy("departement").avg("salary").show();
        df2.groupBy("departement").count().show();
        df2.groupBy("departement").max("salary").show();


    }
}
