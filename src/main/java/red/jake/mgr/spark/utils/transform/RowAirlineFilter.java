package red.jake.mgr.spark.utils.transform;

import red.jake.mgr.spark.model.RowAirline;
import scala.Function1;
import scala.Serializable;

public class RowAirlineFilter implements Function1<RowAirline, Object>, Serializable {
    @Override
    public Object apply(RowAirline rowAirline) {
        return rowAirline.year.equals("2008");
    }
}
