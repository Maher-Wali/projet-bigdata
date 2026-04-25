package projet.bigdata.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class TripStreamJob {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        if (args.length < 2) {
            System.err.println("Usage: TripStreamJob <host> <port>");
            System.exit(1);
        }
        new TripStreamJob().run(args[0], Integer.parseInt(args[1]));
    }

    public void run(String host, int port) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("TripStream")
                .getOrCreate();

        // Each line arriving on the socket is one CSV row from fact_trips_final
        // Schema: data_id, localizacao_partida_id, localizacao_chegada_id, clima_id, distancia_viagem, valor_total
        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .load();

        Dataset<Row> trips = lines.select(
                split(col("value"), ",").getItem(1).cast("int").as("pickup_zone_id"),
                split(col("value"), ",").getItem(4).cast("double").as("distance"),
                split(col("value"), ",").getItem(5).cast("double").as("fare")
        ).filter(col("fare").isNotNull());

        Dataset<Row> result = trips
                .groupBy("pickup_zone_id")
                .agg(
                        count("*").as("trip_count"),
                        round(sum("fare"), 2).as("total_revenue"),
                        round(avg("fare"), 2).as("avg_fare")
                )
                .orderBy(desc("total_revenue"));

        StreamingQuery query = result.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        query.awaitTermination();
    }
}
