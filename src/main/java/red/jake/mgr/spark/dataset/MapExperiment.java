package red.jake.mgr.spark.dataset;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.DatasetPrinter;

import static org.apache.spark.sql.functions.*;

public class MapExperiment extends BaseJob {

    public MapExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        MapExperiment experiment = new MapExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        Dataset<Row> airlines = SourceFactory.getAirlineDataset(session, EnvironmentType.valueOf(envType));
        Dataset<Row> datedAirlines = airlines
                .withColumn("DayofMonth", uniqueString("DayofMonth"))
                .withColumn("Month", uniqueString("Month"))
                .withColumn("Date", to_date(concat_ws("-", col("Year")
                        , col("Month")
                        , col("DayofMonth"))))
                .drop("DayofMonth", "Month", "Year");
        datedAirlines.foreach(new DatasetPrinter());
    }

    private Column uniqueString(String partOfDate) {
        return when(length(col(partOfDate)).lt(2), concat(lit(0), col(partOfDate))).otherwise(col(partOfDate));
    }

}
