package red.jake.mgr.spark.utils.transform;

import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;

public class Printer<T> implements VoidFunction<T>, Serializable {
    @Override
    public void call(T t) {
        System.out.println(t);
    }
}
