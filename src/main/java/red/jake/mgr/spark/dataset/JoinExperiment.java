package red.jake.mgr.spark.dataset;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowCarrier;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.DatasetPrinter;
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
        Dataset<Row> airlines = SourceFactory.getAirlineDataset(session, EnvironmentType.valueOf(envType));
        Dataset<Row> carriers = SourceFactory.getCarrierDataset(session);
        Dataset<Row> joined = airlines.join(functions.broadcast(carriers), functions.col("UniqueCarrier").equalTo(functions.col("code")));
        joined.foreach(new DatasetPrinter());
    }
}
