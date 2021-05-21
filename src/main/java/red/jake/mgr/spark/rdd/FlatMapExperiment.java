package red.jake.mgr.spark.rdd;

import org.apache.spark.api.java.JavaRDD;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowDelayType;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.DelayFlatMap;
import red.jake.mgr.spark.utils.transform.Printer;

public class FlatMapExperiment extends BaseJob {

    public FlatMapExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        FlatMapExperiment experiment = new FlatMapExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        JavaRDD<RowAirline> airlines = SourceFactory.getAirlineJavaRdd(context, EnvironmentType.valueOf(envType));
        JavaRDD<RowDelayType> flatted = airlines.flatMap(new DelayFlatMap());
        flatted.foreach(new Printer<>());
    }
}