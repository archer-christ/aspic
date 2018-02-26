package com.cldellow.aspic.core;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

public class AspicReaderTest {
    @Test
    @Ignore
    public void test() throws Exception {
        String file = "/home/cldellow/Downloads/test.aspic";
        if (!(new File(file)).exists())
            return;
        AspicReader r = new AspicReader(file);

        int iters = 1;
        for (int k = 0; k < iters; k++) {
            long ms = System.nanoTime();
            for (int i = 0; i < iters; i++) {


                AspicRowGroup arg = new AspicRowGroup(
                        r.getFile(),
                        r.getEnumValues(),
                        r.getTypes(),
                        r.getRowGroupOffset(i),
                        r.getRowGroupLength(i));
                for (int row = 0; row < 10000; row++) {
                    arg.setRow(row);
                    long val = arg.getLong(0);
                    long val2 = arg.getLong(1);
                    long val3 = arg.getLong(2);
                    String str = arg.getString(3);
                    float f = arg.getFloat(4);
                    System.out.println(val + " " + str + " " + f);
//                    System.out.println(arg.getLong(0) + ", " + arg.getLong(1));
                }
            }
            System.out.println(System.nanoTime() - ms);
        }
    }

    @Test
    @Ignore
    public void testBig() throws Exception {
        String file = "/home/cldellow/Downloads/big.aspic";
        if (!(new File(file)).exists())
            return;
        AspicReader r = new AspicReader(file);

        long ms = System.currentTimeMillis();
        for (int i = 0; i < r.getNumRowGroups(); i++) {
            System.out.println("group " + i);
            AspicRowGroup arg = new AspicRowGroup(
                    r.getFile(),
                    r.getEnumValues(),
                    r.getTypes(),
                    r.getRowGroupOffset(i),
                    r.getRowGroupLength(i));
            for (int row = 0; row < arg.getNumRows(); row++) {
                arg.setRow(row);
                long val = arg.getLong(0);
                long val2 = arg.getLong(1);
                long val3 = arg.getLong(2);
                String str = arg.getString(3);
                float f = arg.getFloat(4);
//                System.out.println(val + " " + str + " " + f);
//                    System.out.println(arg.getLong(0) + ", " + arg.getLong(1));
            }
        }
        System.out.println(System.currentTimeMillis() - ms);
    }

    @Test
    public void test117g() throws Exception {
        String file = "/home/cldellow/Downloads/117g.aspic";
        if (!(new File(file)).exists())
            return;
        AspicReader r = new AspicReader(file);

        long ms = System.currentTimeMillis();
        for (int i = 0; i < r.getNumRowGroups(); i++) {
            System.out.println("group " + i);
            AspicRowGroup arg = new AspicRowGroup(
                    r.getFile(),
                    r.getEnumValues(),
                    r.getTypes(),
                    r.getRowGroupOffset(i),
                    r.getRowGroupLength(i));
            for (int row = 0; row < arg.getNumRows(); row++) {
                arg.setRow(row);
                long val = arg.getLong(0);
                long val2 = arg.getLong(1);
                long val3 = arg.getLong(2);
                String str = arg.getString(3);
                float f = arg.getFloat(4);
//                System.out.println(val + " " + str + " " + f);
//                    System.out.println(arg.getLong(0) + ", " + arg.getLong(1));
            }
        }
        System.out.println(System.currentTimeMillis() - ms);
    }
}