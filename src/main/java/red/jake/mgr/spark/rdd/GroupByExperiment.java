package red.jake.mgr.spark.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowYearCount;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.Printer;
import red.jake.mgr.spark.utils.transform.ReduceGroup;

public class GroupByExperiment extends BaseJob {

    public GroupByExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        GroupByExperiment experiment = new GroupByExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        JavaRDD<RowAirline> airlines = SourceFactory.getAirlineTypedSource(context, EnvironmentType.valueOf(envType));
        JavaPairRDD<String, Iterable<RowAirline>> groups = airlines.groupBy(row -> row.year);
        JavaRDD<RowYearCount> yearCount = groups.map(new ReduceGroup());
        yearCount.foreach(new Printer<>());
    }
}
