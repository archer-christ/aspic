package com.cldellow.aspic.core;

public interface Stats {
    int getRows();
    long getMinLong(int col);
    long getMaxLong(int col);
    float getMinFloat(int col);
    float getMaxFloat(int col);
    String getMinString(int col);
    String getMaxString(int col);
    int getNulls(int col);
    int getUnique(int col);
}
