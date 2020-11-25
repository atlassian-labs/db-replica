package com.atlassian.db.replica.internal.util;

public class Comparables {
    public static <T extends Comparable<? super T>> T max(T c1, T c2) {
        if (c1 == c2) {
            return c1;
        } else if (c1 == null) {
            return c2;
        } else if (c2 == null) {
            return c1;
        } else {
            return c1.compareTo(c2) > 0 ? c1 : c2;
        }
    }

}
