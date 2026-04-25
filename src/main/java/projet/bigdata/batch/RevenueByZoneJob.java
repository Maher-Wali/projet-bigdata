package projet.bigdata.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class RevenueByZoneJob {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: RevenueByZoneJob <fact_trips_path> <dim_localizacao_path> <output_path>");
            System.exit(1);
        }
        new RevenueByZoneJob().run(args[0], args[1], args[2]);
    }

    public void run(String factsPath, String localizacaoPath, String outputPath) {
        SparkSession spark = SparkSession.builder()
                .appName("RevenueByZone")
                .getOrCreate();

        Dataset<Row> facts = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(factsPath);

        Dataset<Row> locations = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(localizacaoPath);

        Dataset<Row> result = facts
                .join(locations,
                        facts.col("localizacao_partida_id").equalTo(locations.col("localizacao_id")))
                .groupBy("bairro", "zona")
                .agg(
                        count("*").as("total_trips"),
                        round(sum("valor_total"), 2).as("total_revenue"),
                        round(avg("valor_total"), 2).as("avg_fare")
                )
                .orderBy(desc("total_revenue"));

        result.write()
                .option("header", "true")
                .csv(outputPath);

        spark.stop();
    }
}
