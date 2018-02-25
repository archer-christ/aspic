package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.*;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class CsvSchemaInfererTest {
    public String path(String p) {
        String rv = getClass().getResource(p).toString();
        if (rv.startsWith("file:"))
            rv = rv.substring("file:".length());
        return rv;
    }

    @Test
    public void easy1() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/easy-1.csv"));

        CsvSchema fs = fsb.schema;
        assertEquals(0, fs.getRows());
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
        assertEquals("quux", fs.getFields().get(3).getName());
        assertEquals("frob", fs.getFields().get(4).getName());
        assertEquals("bob", fs.getFields().get(5).getName());
    }

    @Test
    public void allTypes() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/easy-2.csv"));

        CsvSchema fs = fsb.schema;
        assertEquals(1, fs.getRows());
        assertEquals(TinyintType.TINYINT, fs.getFields().get(0).getType());
        assertEquals(RealType.REAL, fs.getFields().get(1).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(2).getType());
        assertEquals(DateType.DATE, fs.getFields().get(3).getType());
        assertEquals(BooleanType.BOOLEAN, fs.getFields().get(4).getType());
        assertEquals(TimestampType.TIMESTAMP, fs.getFields().get(5).getType());
        assertEquals(BigintType.BIGINT, fs.getFields().get(6).getType());
        assertEquals(SmallintType.SMALLINT, fs.getFields().get(7).getType());
        assertEquals(IntegerType.INTEGER, fs.getFields().get(8).getType());
    }

    @Test
    public void nulls1() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/nulls-1.csv"));

        CsvSchema fs = fsb.schema;
        assertEquals(2, fs.getRows());
        assertEquals(TinyintType.TINYINT, fs.getFields().get(0).getType());
        assertEquals(RealType.REAL, fs.getFields().get(1).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(2).getType());
        assertEquals(DateType.DATE, fs.getFields().get(3).getType());
        assertEquals(BooleanType.BOOLEAN, fs.getFields().get(4).getType());
        assertEquals(TimestampType.TIMESTAMP, fs.getFields().get(5).getType());
        assertEquals(BigintType.BIGINT, fs.getFields().get(6).getType());
        assertEquals(SmallintType.SMALLINT, fs.getFields().get(7).getType());
        assertEquals(IntegerType.INTEGER, fs.getFields().get(8).getType());
    }

    @Test
    public void nulls2() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/nulls-1.csv"));

        CsvSchema fs = fsb.schema;
        assertEquals(2, fs.getRows());
        assertEquals(TinyintType.TINYINT, fs.getFields().get(0).getType());
        assertEquals(RealType.REAL, fs.getFields().get(1).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(2).getType());
        assertEquals(DateType.DATE, fs.getFields().get(3).getType());
        assertEquals(BooleanType.BOOLEAN, fs.getFields().get(4).getType());
        assertEquals(TimestampType.TIMESTAMP, fs.getFields().get(5).getType());
        assertEquals(BigintType.BIGINT, fs.getFields().get(6).getType());
        assertEquals(SmallintType.SMALLINT, fs.getFields().get(7).getType());
        assertEquals(IntegerType.INTEGER, fs.getFields().get(8).getType());
    }

    @Test
    public void varchar() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/varchars.csv"));

        CsvSchema fs = fsb.schema;
        assertEquals(2, fs.getRows());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(0).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(1).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(2).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(3).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(4).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(5).getType());
        assertEquals(VarcharType.VARCHAR, fs.getFields().get(6).getType());
    }


    @Test
    public void easy4() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/easy-4.csv"));
        CsvSchema fs = fsb.schema;
        assertFalse(fs.getByteOrderMark());
        assertEquals(3, fs.getRows());
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
        assertEquals("quux", fs.getFields().get(3).getName());
        assertEquals("frob", fs.getFields().get(4).getName());
        assertEquals("bob", fs.getFields().get(5).getName());

    }

    @Test
    public void byteOrderMark() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/bom-1.csv"));

        CsvSchema fs = fsb.schema;
        assertTrue(fs.getByteOrderMark());
        assertEquals(2, fs.getFields().size());
        assertEquals("CENSUS_YEAR", fs.getFields().get(0).getName());
        assertEquals("GEO_CODE (POR)", fs.getFields().get(1).getName());
    }

    @Test
    public void dosLineEndings() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/dos-line-endings.csv"));

        CsvSchema fs = fsb.schema;
//        assertEquals("\r\n", fs.getLineSeparator());
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
    }

    @Test
    public void unixLineEndings() {
        CsvSchemaInferer fsb = new CsvSchemaInferer(path("/easy-4.csv"));

        CsvSchema fs = fsb.schema;
//        assertEquals("\n", fs.getLineSeparator());
    }

    //@Test
    public void easy5() {
        CsvSchemaInferer fsb = new CsvSchemaInferer("/tmp/tmphive/rent/big.csv");

        CsvSchema fs = fsb.schema;
        assertEquals(3, fs.getRows());
        assertEquals("foo", fs.getFields().get(0).getName());
        assertEquals("bar", fs.getFields().get(1).getName());
        assertEquals("baz", fs.getFields().get(2).getName());
        assertEquals("quux", fs.getFields().get(3).getName());
        assertEquals("frob", fs.getFields().get(4).getName());
        assertEquals("bob", fs.getFields().get(5).getName());
    }

    @Ignore
    @Test
    public void writeMetas() throws Exception {
        String[] files = new String[]{
                "/tmp/tmphive/small.csv"
                //    ,"/tmp/tmphive/rent/big.csv"
        };

        for (String f : files) {
            if (new File(f).exists()) {
                CsvSchemaInferer fsb = new CsvSchemaInferer(f);

//                String js = Json.FILE_STATS_CODEC.toJson(fsb.stats);
//                PrintWriter pw = new PrintWriter(f + ".metadata");
//                pw.println(js);
//                pw.close();
            }
        }
    }
}