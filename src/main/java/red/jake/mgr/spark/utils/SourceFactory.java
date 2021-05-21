package red.jake.mgr.spark.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowCarrier;
import red.jake.mgr.spark.utils.transform.MapToPojo;

import static red.jake.mgr.spark.utils.SourceMetadata.*;

public class SourceFactory<T> {
    private SourceFactory() {
    }

    public static JavaRDD<RowAirline> getAirlineTypedSource(JavaSparkContext context, EnvironmentType envType) {
        String path = envType == EnvironmentType.LOCAL ? SAMPLE_AIRLINE.getPath() : AIRLINE.getPath();
        return new SourceFactory<RowAirline>().getTypedSource(context, path, RowAirline.class);
    }

    public static JavaRDD<RowCarrier> getCarrierTypedSource(JavaSparkContext context) {
        return new SourceFactory<RowCarrier>().getTypedSource(context, CARRIER.getPath(), RowCarrier.class);
    }

    private JavaRDD<T> getTypedSource(JavaSparkContext context, String path, Class<T> c) {
        JavaRDD<String> stringJavaRDD = context.textFile(path, 1);
        String first = stringJavaRDD.first();
        return stringJavaRDD
                .filter(row -> !first.equals(row))
                .map(new MapToPojo<>(c));
    }
}
