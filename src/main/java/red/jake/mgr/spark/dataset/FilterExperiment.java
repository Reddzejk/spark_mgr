package red.jake.mgr.spark.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.DatasetPrinter;

import static org.apache.spark.sql.functions.col;

public class FilterExperiment extends BaseJob {

    public FilterExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        FilterExperiment experiment = new FilterExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        Dataset<Row> airlines = SourceFactory.getAirlineDataset(session, EnvironmentType.valueOf(envType));
        Dataset<Row> filtered = airlines.filter(col("Year").equalTo("2008"));
        filtered.foreach(new DatasetPrinter());
    }
}
