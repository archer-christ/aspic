package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.*;

public class CsvSchemaTest {
    @Test
    public void serde() {
        CsvSchema fs = new CsvSchema(
                true,
                ImmutableList.copyOf(new Field[]{
                        new Field("foo", VarcharType.VARCHAR),
                        new Field("bar", BigintType.BIGINT)}),
                '|',
                2,
                new String[][]{null, new String[] { "ok" }});

        assertEquals(true, fs.getByteOrderMark());
        assertEquals(ImmutableList.copyOf(new Field[]{
                new Field("foo", VarcharType.VARCHAR),
                new Field("bar", BigintType.BIGINT)}), fs.getFields());
        assertEquals('|', fs.getFieldSeparator());
        assertEquals(2, fs.getRows());
        assertNull(fs.getEnumValues()[0]);
        assertArrayEquals(new String[] {"ok"}, fs.getEnumValues()[1]);
    }
}