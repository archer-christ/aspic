package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AspicWriterTest {
    @Test
    public void go() throws IOException {
        String csvFile = "/home/cldellow/Downloads/test.csv";
        if (!(new File(csvFile).exists()))
            return;
        CsvSchemaInferer inf = new CsvSchemaInferer(csvFile);
        System.out.println(inf.schema);

        CsvSchema schema = inf.schema
                .withFieldType(12, RealType.REAL)
                .withFieldType(13, IntegerType.INTEGER)
                .withFieldType(14, IntegerType.INTEGER);
        System.out.println(schema);
        new AspicWriter(csvFile, schema, 10000, "/home/cldellow/Downloads/test.aspic");
    }
}