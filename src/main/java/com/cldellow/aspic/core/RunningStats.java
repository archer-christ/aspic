package com.cldellow.aspic.core;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;

import java.nio.charset.Charset;
import java.util.List;

public class RunningStats implements Stats {

    private final List<Type> types;
    private final int[] nulls;
    private final long[] minLong;
    private final long[] maxLong;
    private final float[] minFloat;
    private final float[] maxFloat;
    private final String[] minString;
    private final String[] maxString;
    private final HyperLogLogPlus[] uniques;
    private final Charset UTF8 = Charset.forName("UTF-8");
    private int rows = 0;

    public RunningStats(List<Type> types) {
        this.types = types;
        int numColumns = types.size();
        nulls = new int[numColumns];
        minLong = new long[numColumns];
        maxLong = new long[numColumns];
        minFloat = new float[numColumns];
        maxFloat = new float[numColumns];
        minString = new String[numColumns];
        maxString = new String[numColumns];
        uniques = new HyperLogLogPlus[numColumns];

        for (int i = 0; i < numColumns; i++) {
            minLong[i] = Long.MAX_VALUE;
            maxLong[i] = Long.MIN_VALUE;
            minFloat[i] = Float.MAX_VALUE;
            maxFloat[i] = Float.MIN_VALUE;
            uniques[i] = new HyperLogLogPlus(5);
        }
    }

    public void addRow() {
        rows++;
    }

    public void countUnique(int col, String s) {
        uniques[col].offer(s.getBytes(UTF8));
    }

    public void addString(int col, String s) {
        if(minString[col] == null) {
            minString[col] = s;
        } else {
            if(s.compareTo(minString[col]) < 0)
                minString[col] = s;
        }

        if(maxString[col] == null) {
            maxString[col] = s;
        } else {
            if(s.compareTo(maxString[col]) > 0)
                maxString[col] = s;
        }
    }


    public void addLong(int col, long l) {
        if(l > maxLong[col])
            maxLong[col] = l;

        if(l < minLong[col])
            minLong[col] = l;
    }

    public void addFloat(int col, float f) {
        if(f > maxFloat[col])
            maxFloat[col] = f;
        if(f < minFloat[col])
            minFloat[col] = f;
    }

    public int getRows() { return rows; }

    public long getMinLong(int col) {
        return minLong[col];
    }

    public long getMaxLong(int col) {
        return maxLong[col];
    }

    public float getMinFloat(int col) {
        return minFloat[col];
    }

    public float getMaxFloat(int col) {
        return maxFloat[col];
    }

    public String getMinString(int col) {
        return minString[col];
    }

    public String getMaxString(int col) {
        return maxString[col];
    }

    public int getNulls(int col) {
        return nulls[col];
    }

    public int getUnique(int col) {
        return (int)uniques[col].cardinality();
    }
}
