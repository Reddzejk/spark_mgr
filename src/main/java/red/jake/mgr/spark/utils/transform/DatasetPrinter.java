package red.jake.mgr.spark.utils.transform;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

public class DatasetPrinter implements ForeachFunction<Row> {
    @Override
    public void call(Row o) {
        System.out.println(o);
    }
}
