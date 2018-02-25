/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cldellow.aspic.core;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class CsvSchemaInferer {
    public static final int DEFAULT_ROW_GROUP_SIZE = 100000;
    public final CsvSchema schema;
    private final Charset UTF8 = Charset.forName("UTF-8");

//    private static final Pattern longPattern = Pattern.compile("^-?[0-9]+$");
    private static final RunAutomaton longPattern = re("-[0-9][0-9]*|[0-9][0-9]*");
//    private static final Pattern doublePattern = Pattern.compile("^-?[0-9]+\\.[0-9]+$");
    private static final RunAutomaton doublePattern = re("-[0-9][0-9]*\\.[0-9][0-9]*|[0-9][0-9]*\\.[0-9][0-9]*");

//    private static final Pattern datePattern = Pattern.compile("^[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}$");
    private static final RunAutomaton datePattern = re("[0-9][0-9]*-[0-9][0-9]*-[0-9][0-9]*");
    // basicDateTime     yyyyMMdd'T'HHmmss.SSSZ
    // basicDateTimeNoMs yyyyMMdd'T'HHmmssZ
    // dateTime          yyyy-MM-dd'T'HH:mm:ss.SSSZZ
    // dateTimeNoMillis  yyyy-MM-dd'T'HH:mm:ssZZ
//    private static final Pattern dateTimePattern = Pattern.compile(
//            "^[0-9]{1,4}-?[0-9]{1,2}-?[0-9]{1,2}T[0-9]{2}:?[0-9]{2}:?[0-9]{2}.*$"
//    );
    private static final RunAutomaton dateTimePattern = re(
    "[0-9][0-9]*-*[0-9][0-9]*-*[0-9][0-9]*T[0-9]*:*[0-9][0-9]*:*[0-9][0-9]*.*"
            );

    private static RunAutomaton re(String s) {
        return new RunAutomaton(new RegExp(s).toAutomaton());
    }

    CsvSchemaInferer(String fileName) {
        final byte[] bytes = new byte[65536];
        final ByteBuffer buffer;
        final MmapRecord record = new MmapRecord(bytes);

        boolean unixNewline = true;
        final CsvCursor cursor;

        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r");
             FileChannel channel = raf.getChannel();
        ) {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
            // Do a pass over the file, building:
            //   offset of every Nth row start
            //   # of columns per row
            //   for each column, # of entries that can be interpreted as type X (will be used for inference)
            //   for each column, # of NULL entries
            //   for each group of N rows, stats

            boolean byteOrderMark = false;
            if (raf.length() >= 3 && buffer.get(0) == (byte) 0xEF && buffer.get(1) == (byte) 0xBB && buffer.get(2) == (byte) 0xBF) {
                buffer.position(3);
                byteOrderMark = true;
            } else {
                buffer.position(0);
            }

            boolean foundPipe = false;
            boolean foundTab = false;
            byte b = -1;
            while ((b = buffer.get()) != '\n') {
                if (b == '\t')
                    foundTab = true;
                else if (b == '|')
                    foundPipe = true;
            }

            if (buffer.get(buffer.position() - 2) == '\r') {
                unixNewline = false;
            }

            if (byteOrderMark)
                buffer.position(3);
            else
                buffer.position(0);

            CsvReader reader = new CsvReader();
            reader.setContainsHeader(true);
            reader.setErrorOnDifferentFieldCount(true);
            char fieldSeparator = ',';
            if (foundTab)
                fieldSeparator = '\t';
            else if (foundPipe)
                fieldSeparator = '|';

            reader.setFieldSeparator(fieldSeparator);

            FileInputStream fis = new FileInputStream(fileName);
            if (byteOrderMark)
                fis.skip(3);

            Reader r = new BufferedReader(new FileReader(fis.getFD()));
            CsvParser parser = reader.parse(r);
            CsvRow row = parser.nextRow();
            List<String> header = parser.getHeader();
            List<Long> rowGroupOffsets = new Vector<>();

            final int numColumns = header.size();

            DateTimeFormatter[] dateTimeFormatters = new DateTimeFormatter[] {
                    ISODateTimeFormat.dateTimeNoMillis(),
                    ISODateTimeFormat.dateTime(),
                    ISODateTimeFormat.basicDateTime(),
                    ISODateTimeFormat.basicDateTimeNoMillis()
            };

            // SimpleDateFormat inFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
//            DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTimeNoMillis();
//            DateTime dt = formatter.parseDateTime(string);

            int rows = 0;
            int[] nulls = new int[numColumns];
            int[] ints = new int[numColumns];
            int[] floats = new int[numColumns];
            int[] longs = new int[numColumns];
            int[] dates = new int[numColumns];
            int[] timestamps = new int[numColumns];
            int[] bools = new int[numColumns];
            int[] _bytes = new int[numColumns];
            int[] shorts = new int[numColumns];
            Object[] uniqueStrings = new Object[numColumns];
            // The limit beyond which we don't track strings.
            int UNIQUE_CUTOFF = 10000;
            int[] uniqueStringCount = new int[numColumns];
            for(int i = 0; i < numColumns; i++)
                uniqueStrings[i] = new HashSet<String>();

            while (row != null) {
                for (int i = 0; i < numColumns; i++) {
                    String value = row.getField(i);
                    if(uniqueStringCount[i] <= UNIQUE_CUTOFF) {
                        ((Set<String>)uniqueStrings[i]).add(value);
                        uniqueStringCount[i] = ((Set<String>)uniqueStrings[i]).size();
                    }
                    boolean isLong = false;
                    if (value.isEmpty()) {
                        nulls[i]++;
                    } else {

                        if(longPattern.run(value)) {
                            try {
                                long l = Long.parseLong(value);
                                isLong = true;

                                longs[i]++;
                                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE)
                                    ints[i]++;

                                if(l >= Short.MIN_VALUE && l <= Short.MAX_VALUE)
                                    shorts[i]++;

                                if(l >= Byte.MIN_VALUE && l <= Byte.MAX_VALUE)
                                    _bytes[i]++;
                            } catch (NumberFormatException nfe) {

                            }
                        }

                        if(!isLong && doublePattern.run(value)) {
                            try {
                                float f = Float.parseFloat(value);
                                floats[i]++;
                            } catch (NumberFormatException nfe) {

                            }
                        }

                        boolean isYes = isYes(value);

                        boolean isNo = value.equals("0") || value.equals("F") || value.equals("f") ||
                                value.equals("N") || value.equals("n") || value.equals("FALSE") || value.equals("false") ||
                                value.equals("NO") || equals("no");

                        if (isYes || isNo) {
                            bools[i]++;
                        }

                        if(datePattern.run(value)) {
                            try {
                                dateFormatter.parse(value);
                                dates[i]++;
                            } catch (ParseException pe) {
                            }
                        }

                        if(dateTimePattern.run(value)) {
                            for (int j = 0; j < dateTimeFormatters.length; j++) {
                                try {
                                    DateTime dateTime = dateTimeFormatters[j].parseDateTime(value);
                                    timestamps[i]++;
                                    break;
                                } catch (IllegalArgumentException iae) {

                                }
                            }
                        }
                    }
                }
                rows++;
                row = parser.nextRow();
            }

            List<Field> fields = new Vector<>();
            String[][] enumValues = new String[numColumns][];
            for (int i = 0; i < numColumns; i++) {
                Type type = VarcharType.VARCHAR;

                if(floats[i] > 0 && nulls[i] + floats[i] == rows) {
                    type = RealType.REAL;
                } else if(bools[i] > 0 && nulls[i] + bools[i] == rows) {
                    type = BooleanType.BOOLEAN;
                } else if(_bytes[i] > 0 && nulls[i] + _bytes[i] == rows) {
                    type = TinyintType.TINYINT;
                } else if(shorts[i] > 0 && nulls[i] + shorts[i] == rows) {
                    type = SmallintType.SMALLINT;
                } else if(ints[i] > 0 && nulls[i] + ints[i] == rows) {
                    type = IntegerType.INTEGER;
                } else if(longs[i] > 0 && nulls[i] + longs[i] == rows) {
                    type = BigintType.BIGINT;
                } else if(dates[i] > 0 && nulls[i] + dates[i] == rows) {
                    type = DateType.DATE;
                } else if(timestamps[i] > 0 && nulls[i] + timestamps[i] == rows) {
                    type = TimestampType.TIMESTAMP;
                }
                if(type == VarcharType.VARCHAR && ((Set<String>)uniqueStrings[i]).size() < UNIQUE_CUTOFF) {
                    String[] values = ((Set<String>)uniqueStrings[i]).toArray(new String[] {});
                    Arrays.sort(values);
                    enumValues[i] = values;
                }

                fields.add(new Field(header.get(i), type));
            }

            schema = new CsvSchema(byteOrderMark, ImmutableList.copyOf(fields), fieldSeparator, rows, enumValues);
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException(fnfe);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static boolean isYes(String value) {
        return value.equals("1") || value.equals("T") || value.equals("t") ||
                value.equals("Y") || value.equals("y") || value.equals("TRUE") || value.equals("true") ||
                value.equals("YES") || value.equals("yes");
    }
}