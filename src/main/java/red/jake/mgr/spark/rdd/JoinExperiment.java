package red.jake.mgr.spark.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowCarrier;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.Printer;
import scala.Tuple2;

public class JoinExperiment extends BaseJob {

    public JoinExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        JoinExperiment experiment = new JoinExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        JavaRDD<RowAirline> airlines = SourceFactory.getAirlineJavaRdd(context, EnvironmentType.valueOf(envType));
        JavaRDD<RowCarrier> carriers = SourceFactory.getCarrieraJavaRdd(context);
        JavaPairRDD<String, RowAirline> right = airlines.mapToPair((PairFunction<RowAirline, String, RowAirline>) rowAirline -> new Tuple2<>(rowAirline.uniqueCarrier, rowAirline));
        JavaPairRDD<String, RowCarrier> left = carriers.mapToPair((PairFunction<RowCarrier, String, RowCarrier>) rowCarrier -> new Tuple2<>(rowCarrier.code, rowCarrier));
        Broadcast<JavaPairRDD<String, RowCarrier>> broadcast = context.broadcast(left);
        JavaPairRDD<String, Tuple2<RowAirline, RowCarrier>> joined = right.join(broadcast.getValue());
        joined.foreach(new Printer<>());
    }
}
