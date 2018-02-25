package com.cldellow.aspic.core;

public class FixedStats implements Stats {
    private final int rows;
    private final int[] nulls;
    private final int[] uniques;
    private final long[] minLong;
    private final long[] maxLong;
    private final float[] minFloat;
    private final float[] maxFloat;
    private final String[] minString;
    private final String[] maxString;

    public FixedStats(
            int rows,
            int[] nulls,
            int[] uniques,
            long[] minLong,
            long[] maxLong,
            float[] minFloat,
            float[] maxFloat,
            String[] minString,
            String[] maxString) {
        this.rows = rows;
        this.nulls = nulls;
        this.uniques = uniques;
        this.minLong = minLong;
        this.maxLong = maxLong;
        this.minFloat = minFloat;
        this.maxFloat = maxFloat;
        this.minString = minString;
        this.maxString = maxString;
    }


    @Override
    public int getRows() {
        return rows;
    }

    @Override
    public long getMinLong(int col) {
        return minLong[col];
    }

    @Override
    public long getMaxLong(int col) {
        return maxLong[col];
    }

    @Override
    public float getMinFloat(int col) {
        return minFloat[col];
    }

    @Override
    public float getMaxFloat(int col) {
        return maxFloat[col];
    }

    @Override
    public String getMinString(int col) {
        return minString[col];
    }

    @Override
    public String getMaxString(int col) {
        return maxString[col];
    }

    @Override
    public int getNulls(int col) {
        return nulls[col];
    }

    @Override
    public int getUnique(int col) {
        return uniques[col];
    }
}
