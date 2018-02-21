package com.cldellow.aspic.spi;

import com.facebook.presto.spi.type.VarcharType;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import io.airlift.slice.Slice;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitQuickcheck.class)
public class AspicRecordCursorTest {
    File f = new File("/tmp/aspic.csv");

    void deleteFile() {
        if (f.exists())
            f.delete();
    }

    @Property(trials = 10)
    public void testCsvSimpleUnix(
            int seed,
            @InRange(min = "1", max = "9") int rows,
            @InRange(min = "1", max = "9") int cols) throws Exception {
        doTest(seed, rows, cols, "\n",
                CSVFormat.newFormat(',')
                        .withRecordSeparator("\n")
                        .withNullString(""));

    }

    @Property(trials = 10)
    public void testCsvSimpleDos(
            int seed,
            @InRange(min = "1", max = "9") int rows,
            @InRange(min = "1", max = "9") int cols) throws Exception {
        doTest(seed, rows, cols, "\r\n",
                CSVFormat.newFormat(',')
                        .withRecordSeparator("\r\n")
                        .withNullString(""));

    }


    String[][] createStrings(Random r, int rows, int cols) {
        String[][] strs = new String[rows][];
        for (int i = 0; i < rows; i++) {
            strs[i] = new String[cols];
            for (int j = 0; j < strs[i].length; j++) {
                int size = r.nextInt(20);
                strs[i][j] = RandomStringUtils.random(
                        size,
                        0,
                        0,
                        true,
                        true,
                        null,
                        r);
                if (strs[i][j] == null)
                    throw new RuntimeException("null not expected");
                //       System.out.println(strs[i][j]);
            }
        }

        return strs;
    }

    void writeCSV(String[][] strs, CSVFormat fmt) throws Exception {
        CSVPrinter p = fmt.print(f, Charset.forName("UTF-8"));
        for (int row = 0; row < strs.length; row++) {
            for (int col = 0; col < strs[row].length; col++) {
                p.print(strs[row][col]);
            }
            p.println();
        }
        p.close();
    }

    void readCSV(String[][] strs, CSVFormat fmt) throws Exception {
        CSVParser p = fmt.parse(new FileReader(f));

        List<CSVRecord> records = p.getRecords();
        assertEquals(strs.length, records.size());
        for (int row = 0; row < strs.length; row++) {
            assertEquals(strs[row].length, records.get(row).size());
            for (int col = 0; col < strs[row].length; col++) {
                // coerce null to empty string
                String str = records.get(row).get(col);
                if (str == null)
                    str = "";
                assertEquals("Apache CSV roundtrip failed?!", strs[row][col], str);
            }
        }
    }

    void doTest(int seed, int rows, int cols, String lineSeparator, CSVFormat fmt) throws Exception {
        deleteFile();
        Random r = new Random(seed);
        String[][] strs = createStrings(r, rows, cols);

        /* Verify the strings roundtrip via the Apache CSV reader
           before we bother seeing if it roundtrips for us.
         */
        writeCSV(strs, fmt);
        readCSV(strs, fmt);

        List<AspicColumnHandle> columns = new Vector<>();
        for (int i = 0; i < cols; i++) {
            columns.add(new AspicColumnHandle("", "col" + i, VarcharType.VARCHAR, i));
        }
        AspicRecordCursor arc = new AspicRecordCursor(
                columns,
                lineSeparator,
                f.getAbsolutePath(),
                0,
                f.length()
        );

//        System.out.println("  reading:");
        //      byte[] encoded = Files.readAllBytes(Paths.get(f.getAbsolutePath()));
        //    System.out.println(new String(encoded, Charset.forName("UTF-8")));
        for (int row = 0; row < rows; row++) {
            assertTrue("expect row " + row, arc.advanceNextPosition());
            for (int col = 0; col < cols; col++) {
                String str = "";
                if (!arc.isNull(col)) {
                    Slice s = arc.getSlice(col);
                    str = new String(s.getBytes(), 0, s.getBytes().length);
                }

                if (str.equals("null"))
                    throw new RuntimeException("weird");
                if (str == null)
                    throw new RuntimeException("weird2");
                assertEquals("row " + row + ", col " + col, strs[row][col], str);
            }
        }
    }
}