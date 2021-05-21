package red.jake.mgr.spark.utils.transform;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import red.jake.mgr.spark.model.AirlineHeader;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowDelayType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class DelayFlatMap implements FlatMapFunction<RowAirline, RowDelayType> {

    @Override
    public Iterator<RowDelayType> call(RowAirline rowAirline) {
        List<RowDelayType> collector = new ArrayList<>();
        computeDelay(rowAirline.flightNum, AirlineHeader.arrDelay, rowAirline.arrDelay).ifPresent(collector::add);
        computeDelay(rowAirline.flightNum, AirlineHeader.depDelay, rowAirline.depDelay).ifPresent(collector::add);
        computeDelay(rowAirline.flightNum, AirlineHeader.nasDelay, rowAirline.nasDelay).ifPresent(collector::add);
        computeDelay(rowAirline.flightNum, AirlineHeader.carrierDelay, rowAirline.carrierDelay).ifPresent(collector::add);
        computeDelay(rowAirline.flightNum, AirlineHeader.weatherDelay, rowAirline.weatherDelay).ifPresent(collector::add);
        computeDelay(rowAirline.flightNum, AirlineHeader.lateAircraftDelay, rowAirline.lateAircraftDelay).ifPresent(collector::add);
        computeDelay(rowAirline.flightNum, AirlineHeader.securityDelay, rowAirline.securityDelay).ifPresent(collector::add);
        return collector.iterator();
    }

    private Optional<RowDelayType> computeDelay(String flightNum, String delayType, String delayStr) {
        int delay = getDelay(delayStr);
        return delay > 0 ? Optional.of(new RowDelayType(flightNum, delayType, delay)) : Optional.empty();
    }

    private int getDelay(String delay) {
        boolean isNumber = NumberUtils.isNumber(delay);
        return isNumber ? Integer.parseInt(delay) : 0;
    }
}

