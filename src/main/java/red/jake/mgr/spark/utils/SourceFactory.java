package red.jake.mgr.spark.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowCarrier;
import red.jake.mgr.spark.utils.transform.MapToPojo;

import static red.jake.mgr.spark.utils.SourceMetadata.*;

public class SourceFactory<T> {
    private SourceFactory() {
    }

    public static JavaRDD<RowAirline> getAirlineJavaRdd(JavaSparkContext context, EnvironmentType envType) {
        String path = envType == EnvironmentType.LOCAL ? SAMPLE_AIRLINE.getPath() : AIRLINE.getPath();
        return new SourceFactory<RowAirline>().getRdd(context, path, RowAirline.class);
    }

    public static JavaRDD<RowCarrier> getCarrieraJavaRdd(JavaSparkContext context) {
        return new SourceFactory<RowCarrier>().getRdd(context, CARRIER.getPath(), RowCarrier.class);
    }

    public static Dataset<Row> getAirlineDataset(SparkSession session, EnvironmentType envType) {
        String path = envType == EnvironmentType.LOCAL ? SAMPLE_AIRLINE.getPath() : AIRLINE.getPath();
        return new SourceFactory<RowAirline>().getDataset(session, path);
    }

    public static Dataset<Row> getCarrierDataset(SparkSession session) {
        return new SourceFactory<RowCarrier>().getDataset(session, CARRIER.getPath());
    }


    private JavaRDD<T> getRdd(JavaSparkContext context, String path, Class<T> c) {
        JavaRDD<String> stringJavaRDD = context.textFile(path, 1);
        String first = stringJavaRDD.first();
        return stringJavaRDD
                .filter(row -> !first.equals(row))
                .map(new MapToPojo<>(c));
    }

    private Dataset<Row> getDataset(SparkSession session, String path) {
        return session.read().option("header", true).csv(path);
    }
}
