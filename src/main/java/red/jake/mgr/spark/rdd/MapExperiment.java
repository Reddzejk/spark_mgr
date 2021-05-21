package red.jake.mgr.spark.rdd;

import org.apache.spark.api.java.JavaRDD;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowAirlineDated;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.AirlineDateMapper;
import red.jake.mgr.spark.utils.transform.Printer;

public class MapExperiment extends BaseJob {

    public MapExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        MapExperiment experiment = new MapExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        JavaRDD<RowAirline> airlines = SourceFactory.getAirlineTypedSource(context, EnvironmentType.valueOf(envType));
        JavaRDD<RowAirlineDated> mapped = airlines.map(new AirlineDateMapper());
        mapped.foreach(new Printer<>());
    }

}
