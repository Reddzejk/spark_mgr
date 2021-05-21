package red.jake.mgr.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import static red.jake.mgr.spark.utils.EnvironmentType.LOCAL;

public abstract class BaseJob {
    protected final String envType;
    protected final JavaSparkContext context;
    protected final SparkSession session;

    public BaseJob(String[] params) {
        envType = getEnvType(params);
        context = new JavaSparkContext(new SparkConf().setAppName("MapExperiment").setMaster("local[*]"));
        session = null;
    }

    private String getEnvType(String[] param) {
        try {
            return param[0].split("=")[1];
        } catch (Exception e) {
            return LOCAL.name();
        }
    }

}
