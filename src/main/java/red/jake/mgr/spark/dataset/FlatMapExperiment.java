package red.jake.mgr.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import red.jake.mgr.spark.BaseJob;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowDelayType;
import red.jake.mgr.spark.utils.EnvironmentType;
import red.jake.mgr.spark.utils.SourceFactory;
import red.jake.mgr.spark.utils.transform.DatasetDelayFlatMap;
import red.jake.mgr.spark.utils.transform.DatasetPrinter;
import red.jake.mgr.spark.utils.transform.DelayFlatMap;
import red.jake.mgr.spark.utils.transform.Printer;

import java.util.Iterator;

public class FlatMapExperiment extends BaseJob {

    public FlatMapExperiment(String[] params) {
        super(params);
    }

    public static void main(String[] args) {
        FlatMapExperiment experiment = new FlatMapExperiment(args);
        experiment.runJob();
    }

    public void runJob() {
        Dataset<Row> airlines = SourceFactory.getAirlineDataset(session, EnvironmentType.valueOf(envType));
        Dataset<Row> flatted = airlines.flatMap(new DatasetDelayFlatMap(), RowEncoder.apply(
                new StructType().add("FlightNum", DataTypes.StringType, true)
                        .add("DelayType", DataTypes.StringType, true)
                        .add("", DataTypes.IntegerType, true)
        ));
        flatted.foreach(new DatasetPrinter());
    }
}