package red.jake.mgr.spark.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.DatasetPrinter;

public class GroupByExperiment extends BaseJob {

    public GroupByExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        GroupByExperiment experiment = new GroupByExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        Dataset<Row> airlines = SourceFactory.getAirlineDataset(session, EnvironmentType.valueOf(envType))
                .withColumn("one", functions.lit(1));
        Dataset<Row> yearCount = airlines.groupBy("Year")
                .agg(functions.sum("one").as("count"));
        yearCount.foreach(new DatasetPrinter());
    }
}
