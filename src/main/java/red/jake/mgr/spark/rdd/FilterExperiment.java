package red.jake.mgr.spark.rdd;

import org.apache.spark.api.java.JavaRDD;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.Printer;

public class FilterExperiment extends BaseJob {

    public FilterExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        FilterExperiment experiment = new FilterExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        JavaRDD<RowAirline> airlines = SourceFactory.getAirlineTypedSource(context, EnvironmentType.valueOf(envType));
        JavaRDD<RowAirline> filtered = airlines.filter(row -> row.year.equals("2008"));
        filtered.foreach(new Printer<>());
    }
}
