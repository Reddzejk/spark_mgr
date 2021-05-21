package red.jake.mgr.spark.utils.transform;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.function.Function;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowYearCount;
import scala.Tuple2;

public class ReduceGroup implements Function<Tuple2<String, Iterable<RowAirline>>, RowYearCount> {

    @Override
    public RowYearCount call(Tuple2<String, Iterable<RowAirline>> row) {
        return new RowYearCount(row._1, Iterables.size(row._2));
    }
}
