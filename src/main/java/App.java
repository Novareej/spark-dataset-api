import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class App {
    public static void main(String[] args) {
        SparkSession sp = SparkSession
                .builder()
                .appName("Spark SQL TP")
                .master("local[*]")
                .getOrCreate();

        //Untyped dataset ( Dataset of rows ) == Dataframe
        Dataset<Row> df = sp.read().option("multiline",true).json("Employes.json");
        df.printSchema();

        //Filtrer employés entre 30 et 35 ans
        df.filter(col("age").between(30,35)).show();

        //Moyenne de salaire de chaque départ'
        df.groupBy("departement").avg("salary").show();

        //Combien de salariés par départ'
        df.groupBy("departement").count().show();

        //Max des salaires de chaque départ
        df.groupBy("departement").max("salary").show();

        
    }
}
