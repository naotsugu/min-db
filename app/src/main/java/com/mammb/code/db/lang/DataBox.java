package com.mammb.code.db.lang;

public interface DataBox<T extends Comparable<T>> extends Comparable<DataBox<T>> {

    T val();

    static <T extends Comparable<T>> DataBox<T> of(T val) {
        DataBox<?> ret = switch (val) {
            case Integer v -> new IntBox(v);
            case String v -> new StrBox(v);
            default -> throw new RuntimeException();
        };
        return (DataBox<T>) ret;
    }

    default int compareTo(DataBox<T> o) {
        return val().compareTo(o.val());
    }

    default int compareLoose(DataBox<?> o) {
        return val().compareTo((T) o.val());
    }

    record IntBox(Integer val) implements DataBox<Integer> { }

    record StrBox(String val) implements DataBox<String> { }

}
