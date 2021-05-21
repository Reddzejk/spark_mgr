package red.jake.mgr.spark.utils.transform;

import com.google.common.collect.Iterators;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Iterator;

public class MapToPojo<T> implements Function<String, T>, Serializable {

    private final Class<T> c;

    public MapToPojo(Class<T> c) {
        this.c = c;
    }

    @Override
    public T call(String row) throws Exception {
        T instance = c.newInstance();
        int startValue = 0;
        int endValue = row.indexOf(",");
        Iterator<Field> iterator = Iterators.forArray(c.getFields());
        while (iterator.hasNext()) {
            String value = row.substring(startValue, endValue);
            startValue = endValue + 1;
            int index = row.indexOf(",", startValue);
            endValue = index > 0 ? index : row.length();
            try {
                iterator.next().set(instance, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return instance;
    }
}
