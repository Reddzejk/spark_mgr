package red.jake.mgr.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.threeten.extra.DayOfMonth;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowAirlineDated;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.AirlineDateMapper;
import red.jake.mgr.spark.utils.transform.DatasetPrinter;
import red.jake.mgr.spark.utils.transform.Printer;

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
                .withColumn("Date", concat_ws("-", col("DayofMonth")
                        , col("Month")
                        , col("Year")))
                .drop("DayofMonth", "Month", "Year");
        datedAirlines.foreach(new DatasetPrinter());
    }

    private Column uniqueString(String partOfDate) {
        return when(length(col(partOfDate)).lt(2), concat(lit(0), col(partOfDate))).otherwise(col(partOfDate));
    }

}
