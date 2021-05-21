package red.jake.mgr.spark.utils.transform;

import org.apache.spark.api.java.function.Function;
import red.jake.mgr.spark.model.RowAirline;
import red.jake.mgr.spark.model.RowAirlineDated;

import java.time.LocalDate;

public class AirlineDateMapper implements Function<RowAirline, RowAirlineDated> {

    @Override
    public RowAirlineDated call(RowAirline rowAirline) {
        RowAirlineDated dated = new RowAirlineDated();
        dated.date = LocalDate.parse(rowAirline.year + "-" + unifyString(rowAirline.month) + "-" + unifyString(rowAirline.dayOfMonth));
        dated.actualElapsedTime = rowAirline.actualElapsedTime;
        dated.actualElapsedTime = rowAirline.actualElapsedTime;
        dated.airTime = rowAirline.airTime;
        dated.arrDelay = rowAirline.arrDelay;
        dated.arrTime = rowAirline.arrTime;
        dated.crsArrTime = rowAirline.crsArrTime;
        dated.crsDepTime = rowAirline.crsDepTime;
        dated.crsElapsedTime = rowAirline.crsElapsedTime;
        dated.cancellationCode = rowAirline.cancellationCode;
        dated.cancelled = rowAirline.cancelled;
        dated.carrierDelay = rowAirline.carrierDelay;
        dated.dayOfWeek = rowAirline.dayOfWeek;
        dated.depDelay = rowAirline.depDelay;
        dated.depTime = rowAirline.depTime;
        dated.dest = rowAirline.dest;
        dated.distance = rowAirline.distance;
        dated.diverted = rowAirline.diverted;
        dated.flightNum = rowAirline.flightNum;
        dated.lateAircraftDelay = rowAirline.lateAircraftDelay;
        dated.nasDelay = rowAirline.nasDelay;
        dated.origin = rowAirline.origin;
        dated.securityDelay = rowAirline.securityDelay;
        dated.tailNum = rowAirline.tailNum;
        dated.taxiIn = rowAirline.taxiIn;
        dated.taxiOut = rowAirline.taxiOut;
        dated.uniqueCarrier = rowAirline.uniqueCarrier;
        dated.weatherDelay = rowAirline.weatherDelay;
        return dated;
    }

    private String unifyString(String unit) {
        return unit.length() < 2 ? "0" + unit : unit;
    }
}

