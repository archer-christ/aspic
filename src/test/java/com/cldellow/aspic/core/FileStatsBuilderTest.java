package com.cldellow.aspic.core;

import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;

public class FileStatsBuilderTest {
    public String path(String p) {
        String rv = getClass().getResource(p).toString();
        if (rv.startsWith("file:"))
            rv = rv.substring("file:".length());
        return rv;
    }

    @Test
    public void easy1() {
        FileStatsBuilder fsb = new FileStatsBuilder(path("/easy-1.csv"), "easy-1", 2);

        FileStats fs = fsb.stats;
        assertEquals("easy-1", fs.getName());
        assertEquals(0, fs.getRows());
        assertEquals(0, fs.getRowGroupOffsets().size());
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
        assertEquals("quux", fs.getFields().get(3).getName());
        assertEquals("frob", fs.getFields().get(4).getName());
        assertEquals("bob", fs.getFields().get(5).getName());
    }

    @Test
    public void easy2() {
        FileStatsBuilder fsb = new FileStatsBuilder(path("/easy-2.csv"), "easy-2", 2);

        FileStats fs = fsb.stats;
        assertEquals(1, fs.getRows());
        assertEquals(1, fs.getRowGroupOffsets().size());
        assertEquals(26L, (long) fs.getRowGroupOffsets().get(0));
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
        assertEquals("quux", fs.getFields().get(3).getName());
        assertEquals("frob", fs.getFields().get(4).getName());
        assertEquals("bob", fs.getFields().get(5).getName());

    }

    @Test
    public void easy4() {
        FileStatsBuilder fsb = new FileStatsBuilder(path("/easy-4.csv"), "easy-4", 2);

        FileStats fs = fsb.stats;
        assertEquals(3, fs.getRows());
        assertEquals(2, fs.getRowGroupOffsets().size());
        assertEquals(26L, (long) fs.getRowGroupOffsets().get(0));
        assertEquals(98L, (long) fs.getRowGroupOffsets().get(1));
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
        assertEquals("quux", fs.getFields().get(3).getName());
        assertEquals("frob", fs.getFields().get(4).getName());
        assertEquals("bob", fs.getFields().get(5).getName());

    }

    @Test
    public void byteOrderMark() {
        FileStatsBuilder fsb = new FileStatsBuilder(path("/bom-1.csv"));

        FileStats fs = fsb.stats;
        assertEquals(2, fs.getFields().size());
        assertEquals("CENSUS_YEAR", fs.getFields().get(0).getName());
        assertEquals("GEO_CODE (POR)", fs.getFields().get(1).getName());
    }

    //@Test
    public void easy5() {
        FileStatsBuilder fsb = new FileStatsBuilder("/tmp/tmphive/rent/big.csv");

        FileStats fs = fsb.stats;
        assertEquals(3, fs.getRows());
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
        assertEquals("quux", fs.getFields().get(3).getName());
        assertEquals("frob", fs.getFields().get(4).getName());
        assertEquals("bob", fs.getFields().get(5).getName());
    }

    @Test
    public void writeMetas() throws Exception {
        String[] files = new String[]{
                "/tmp/tmphive/small.csv"
                //    ,"/tmp/tmphive/rent/big.csv"
        };

        for (String f : files) {
            if (new File(f).exists()) {
                FileStatsBuilder fsb = new FileStatsBuilder(f);

                String js = Json.FILE_STATS_CODEC.toJson(fsb.stats);
                PrintWriter pw = new PrintWriter(f + ".metadata");
                pw.println(js);
                pw.close();
            }
        }
    }
}