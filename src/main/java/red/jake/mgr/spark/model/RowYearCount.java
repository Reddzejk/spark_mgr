package red.jake.mgr.spark.model;

import java.io.Serializable;

public class RowYearCount implements Serializable {
    public String year;
    public int count;

    public RowYearCount() {
    }

    public RowYearCount(String year, int count) {
        this.year = year;
        this.count = count;
    }

    @Override
    public String toString() {
        return String.format("year: %s, count: %s | ", year, count);
    }
}
