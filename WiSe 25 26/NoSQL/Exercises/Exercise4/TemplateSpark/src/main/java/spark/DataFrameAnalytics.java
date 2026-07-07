package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameAnalytics {

    // a)
    public static void printMeteoriteSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("MeteoriteSchema")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> meteorites = spark.read()
                .option("multiLine", true)
                .json("src/main/resources/meteorite.json");

        System.out.println("Inferred schema of meteorite.json:");
        meteorites.printSchema();

        spark.stop();
    }

    public static void main(String[] args) {
        printMeteoriteSchema();
    }
}
