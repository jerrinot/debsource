package info.jerrinot.jet.cdc.impl;

import java.util.Iterator;
import java.util.List;

public final class StringUtils {
    private StringUtils() {

    }

    public static <T> String toCommaSeparated(List<T> list) {
        if (list == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = list.iterator();
        while (iter.hasNext()) {
            T item = iter.next();
            sb.append(item);
            if (iter.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
