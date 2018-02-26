package com.cldellow.aspic.core;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.*;

@RunWith(JUnitQuickcheck.class)
public class RunningStatsTest {
    @Property
    public void testRows(@InRange(min = "0", max="3000") int rows) {
        RunningStats rs = new RunningStats(2);
        for(int i = 0; i < rows; i++)
            rs.addRow();

        assertEquals(rows, rs.getRows());
    }

    @Property
    public void testNulls(@InRange(min = "0", max="3000") int nulls1,
                          @InRange(min = "0", max="3000") int nulls2) {
        RunningStats rs = new RunningStats(2);
        for(int i = 0; i < nulls1; i++)
            rs.addNull(0);
        for(int i = 0; i < nulls2; i++)
            rs.addNull(1);

        assertEquals(nulls1, rs.getNulls(0));
        assertEquals(nulls2, rs.getNulls(1));
    }

    @Property
    public void testUniques(String[] strs1, String[] strs2) {
        RunningStats rs = new RunningStats(2);

        HashSet<String> set1 = new HashSet<>();
        HashSet<String> set2 = new HashSet<>();
        for(String s: strs1) {
            rs.countUnique(0, s);
            set1.add(s);
        }

        for(String s: strs2) {
            rs.countUnique(1, s);
            set2.add(s);
        }

        // should be +/- 5%
        assertTrue("set1 -5%", set1.size() * 0.95 <= rs.getUnique(0));
        assertTrue("set1 +5%", set1.size() * 1.05 >= rs.getUnique(0));
        assertTrue("set2 -5%", set2.size() * 0.95 <= rs.getUnique(1));
        assertTrue("set2 +5%", set2.size() * 1.05 >= rs.getUnique(1));
    }

    @Property
    public void testLongs(long[] ls, long[] ls2) {
        RunningStats rs = new RunningStats(2);
        for(long l: ls)
            rs.addLong(0, l);

        for(long l: ls2)
            rs.addLong(1, l);

        Arrays.sort(ls);
        Arrays.sort(ls2);

        if(ls.length > 0) {
            assertEquals(ls[0], rs.getMinLong(0));
            assertEquals(ls[ls.length - 1], rs.getMaxLong(0));
        }

        if(ls2.length > 0) {
            assertEquals(ls2[0], rs.getMinLong(1));
            assertEquals(ls2[ls2.length - 1], rs.getMaxLong(1));
        }
    }

    @Property
    public void testFloats(float[] fs, float[] fs2) {
        RunningStats rs = new RunningStats(2);
        for(float f: fs)
            rs.addFloat(0, f);

        for(float f: fs2)
            rs.addFloat(1, f);

        Arrays.sort(fs);
        Arrays.sort(fs2);

        if(fs.length > 0) {
            assertEquals(fs[0], rs.getMinFloat(0), 0.001);
            assertEquals(fs[fs.length - 1], rs.getMaxFloat(0), 0.001);
        }

        if(fs2.length > 0) {
            assertEquals(fs2[0], rs.getMinFloat(1), 0.001);
            assertEquals(fs2[fs2.length - 1], rs.getMaxFloat(1), 0.001);
        }
    }

    @Property
    public void testStrings(String[] strs1, String[] strs2) {
        RunningStats rs = new RunningStats(2);
        for(String s: strs1)
            rs.addString(0, s);
        for(String s: strs2)
            rs.addString(1, s);

        Arrays.sort(strs1);
        Arrays.sort(strs2);

        if(strs1.length > 0) {
            assertEquals(strs1[0], rs.getMinString(0));
            assertEquals(strs1[strs1.length - 1], rs.getMaxString(0));
        }

        if(strs2.length > 0) {
            assertEquals(strs2[0], rs.getMinString(1));
            assertEquals(strs2[strs2.length - 1], rs.getMaxString(1));
        }
    }
}