package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import static org.junit.Assert.*;

public class FileStatsTest {
    @Test
    public void serde() {
        //List<String> fieldNames, List<Long> rowGroupOffsets, int rows
        FileStats fs = new FileStats(
                "/some/file",
                "somefile",
                ImmutableList.copyOf(new Field[]{
                        new Field("foo", VarcharType.VARCHAR),
                        new Field("bar", BigintType.BIGINT)}),
                ImmutableList.copyOf(new Long[]{1L, 2L}),
                2);

        String js = Json.FILE_STATS_CODEC.toJson(fs);
        FileStats fs2 = Json.FILE_STATS_CODEC.fromJson(js);


        assertEquals(fs.getFields(), fs2.getFields());
        assertEquals(fs.getRows(), fs2.getRows());
        assertEquals(fs.getRowGroupOffsets(), fs2.getRowGroupOffsets());
        assertEquals(fs.getFile(), fs2.getFile());
        assertEquals(fs.getName(), fs2.getName());
    }
}