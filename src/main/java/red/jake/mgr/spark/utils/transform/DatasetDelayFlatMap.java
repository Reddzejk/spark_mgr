package red.jake.mgr.spark.utils.transform;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import red.jake.mgr.spark.model.AirlineHeader;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowDelayType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class DatasetDelayFlatMap implements FlatMapFunction<Row, Row> {

    @Override
    public Iterator<Row> call(Row row) {
        List<Row> collector = new ArrayList<>();
        computeDelay(row.getAs("FlightNum"), AirlineHeader.arrDelay, row.getAs("ArrDelay")).ifPresent(collector::add);
        computeDelay(row.getAs("FlightNum"), AirlineHeader.depDelay, row.getAs("DepDelay")).ifPresent(collector::add);
        computeDelay(row.getAs("FlightNum"), AirlineHeader.nasDelay, row.getAs("NASDelay")).ifPresent(collector::add);
        computeDelay(row.getAs("FlightNum"), AirlineHeader.carrierDelay, row.getAs("CarrierDelay")).ifPresent(collector::add);
        computeDelay(row.getAs("FlightNum"), AirlineHeader.weatherDelay, row.getAs("WeatherDelay")).ifPresent(collector::add);
        computeDelay(row.getAs("FlightNum"), AirlineHeader.lateAircraftDelay, row.getAs("LateAircraftDelay")).ifPresent(collector::add);
        computeDelay(row.getAs("FlightNum"), AirlineHeader.securityDelay, row.getAs("SecurityDelay")).ifPresent(collector::add);
        return collector.iterator();
    }

    private Optional<Row> computeDelay(String flightNum, String delayType, String delayStr) {
        int delay = getDelay(delayStr);
        return delay > 0 ? Optional.of(RowFactory.create(flightNum, delayType, delay)) : Optional.empty();
    }

    private int getDelay(String delay) {
        boolean isNumber = NumberUtils.isNumber(delay);
        return isNumber ? Integer.parseInt(delay) : 0;
    }
}

