package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.*;
import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.hash.TLongIntHashMap;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

public class AspicWriter {
    final static LZ4Factory factory = LZ4Factory.fastestInstance();
    private final CsvParser parser;
    private final DataOutputStream dos;
    private final ArrayList<Integer> rowGroupOffsets = new ArrayList<>();
    private final ArrayList<Integer> rowGroupRawLength = new ArrayList<>();
    private final ArrayList<Integer> rowGroupCompressedLength = new ArrayList<>();
    private final ArrayList<RunningStats> rowGroupStats = new ArrayList<>();
    private final int numColumns;
    private final Charset UTF8 = Charset.forName("UTF-8");
    private final RunningStats docStats;
    private final CsvSchema schema;
    private final Object[] enum2Ids;
    private final int rowGroupSize;
    private final Type[] types;
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final LZ4Compressor lz4c = factory.highCompressor();
    private int currentRow = 0;

    public AspicWriter(String csvFile, CsvSchema schema, int rowGroupSize, String outputFile) throws FileNotFoundException, IOException {
        this.schema = schema;
        this.rowGroupSize = rowGroupSize;
        System.out.println("rowGroupSize = " + rowGroupSize);
        numColumns = schema.getFields().size();
        enum2Ids = new Object[numColumns];
        types = new Type[numColumns];
        for (int i = 0; i < numColumns; i++) {
            types[i] = schema.getFields().get(i).getType();
            String[] enumValues = schema.getEnumValues()[i];
            if (enumValues != null) {
                Map<String, Integer> enum2Id = new HashMap<>();
                enum2Ids[i] = enum2Id;
                for (int j = 0; j < enumValues.length; j++)
                    enum2Id.put(enumValues[j], j);
            }
        }
        CsvReader reader = new CsvReader();
        reader.setContainsHeader(true);
        reader.setErrorOnDifferentFieldCount(true);
        reader.setFieldSeparator(schema.getFieldSeparator());

        FileInputStream fis = new FileInputStream(csvFile);
        if (schema.getByteOrderMark())
            fis.skip(3);

        Reader r = new BufferedReader(new FileReader(fis.getFD()));
        parser = reader.parse(r);
        CsvRow row = parser.nextRow();

        dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
        // Magic number + version
        dos.writeByte('A');
        dos.writeByte('S');
        dos.writeByte('P');
        dos.writeByte('C');
        dos.writeByte(1);
        // Placeholder for rowgroup indexes.
        dos.writeInt(0);
        // # of columns, their types, their names.
        dos.writeByte(numColumns);
        for (int i = 0; i < numColumns; i++)
            dos.writeByte(TypeSerializer.typeToId(schema.getFields().get(i).getType()));
        for (int i = 0; i < numColumns; i++)
            writeString(dos, schema.getFields().get(i).getName());

        writeEnumValues(dos, schema.getEnumValues());

        docStats = new RunningStats(numColumns);
        writeRowGroups(row);

        int metadataPos = dos.size();

        dos.writeInt(rowGroupStats.size());
        for (int i : rowGroupOffsets) {
            dos.writeInt(i);
        }
        writeStats(dos, docStats);
        for (RunningStats s : rowGroupStats) {
            writeStats(dos, s);
        }

        dos.flush();
        dos.close();

        RandomAccessFile raf = new RandomAccessFile(outputFile, "rw");
        raf.seek(5);
        raf.writeInt(metadataPos);

        for(int i = 0; i < rowGroupRawLength.size(); i++) {
            raf.seek(rowGroupOffsets.get(i));
            raf.writeInt(rowGroupRawLength.get(i));
            raf.writeInt(rowGroupCompressedLength.get(i));
        }
        raf.close();
    }

    private void writeStats(DataOutputStream dos, RunningStats stats) throws IOException {
        dos.writeInt(stats.getRows());

        Set<String> strSet = new HashSet<>();

        // note: it's kind of wasteful to rebuild a strings table since some of them
        // likely exist in the dict string table... buuuuut, it makes processing
        // easier.
        for (int i = 0; i < numColumns; i++) {
            if (types[i].equals(VarcharType.VARCHAR)) {
                strSet.add(stats.getMinString(i));
                strSet.add(stats.getMaxString(i));
            }
        }

        String[] strs = strSet.toArray(new String[]{});
        int strSize = 0;
        for (String str : strs) {
            strSize += 2; // store length as a short
            strSize += str.getBytes(UTF8).length;
        }
        dos.writeShort(strSize + 2);
        dos.writeShort(strs.length);
        for (String str : strs) {
            writeString(dos, str);
        }

        // num null, num unique, min value, max value.
        // for strings... point to a shared string table so we can have constant time lookups
        for (int i = 0; i < numColumns; i++) {
            dos.writeInt(stats.getNulls(i));
            dos.writeInt(stats.getUnique(i));
            if (types[i].equals(VarcharType.VARCHAR)) {
                writeStringReference(dos, strs, stats.getMinString(i));
                writeStringReference(dos, strs, stats.getMaxString(i));
            } else if (types[i].equals(RealType.REAL)) {
                dos.writeLong(Float.floatToRawIntBits(stats.getMinFloat(i)));
                dos.writeLong(Float.floatToRawIntBits(stats.getMaxFloat(i)));
            } else {
                dos.writeLong(stats.getMinLong(i));
                dos.writeLong(stats.getMaxLong(i));
            }
        }
    }

    private void writeStringReference(DataOutputStream dos, String[] strs, String needle) throws IOException {
        for (int i = 0; i < strs.length; i++) {
            if (strs[i].equals(needle)) {
                dos.writeLong(i);
                return;
            }
        }

        throw new IllegalArgumentException("unknown string: " + needle);
    }

    private void writeEnumValues(DataOutputStream dos, String enumValues[][]) throws IOException {
        for (int i = 0; i < enumValues.length; i++) {
            if (enumValues[i] != null) {
                dos.writeShort(enumValues[i].length);
                for (int j = 0; j < enumValues[i].length; j++)
                    writeString(dos, enumValues[i][j]);
            } else {
                dos.writeShort(0);
            }
        }
    }

    private void writeString(DataOutputStream dos, String s) throws IOException {
        byte[] bytes = s.getBytes(UTF8);
        writeBytes(dos, bytes);
    }

    private void writeBytes(DataOutputStream dos, byte[] bytes) throws IOException {
        // just use a short for length; we dictify fields w < 10K strings, so
        // these are probably long strings.
        dos.writeShort(bytes.length);
        dos.write(bytes);
    }

    private void writeRowGroups(CsvRow row) throws IOException {
        RunningStats groupStats = null;
       /* We buffer rowgroup data in this so we can do analysis to decide
       whether to encode a rowgroup differently.

          Note that `long` is sufficient to track longs, shorts, bytes, floats, etc.
       */
        long[][] longs = null;
        String[][] strings = null;

        // Track unique # of numerics in a given rowgroup for each column.
        // If it's <= 256, we'll store it as a dict lookup. This prevents
        // accidental explosion of low-cardinality identifiers and floats
        // from taking 4 bytes per row instead of 1.
        TLongIntHashMap[] uniqueNumerics = null;
        boolean[][] isNull = null;
        boolean[] hasNulls = null;

        while (row != null) {
            int groupRow = currentRow % rowGroupSize;
            if (groupRow == 0) {
                if (currentRow != 0) {
                    writeRowGroup(groupStats, hasNulls, isNull, longs, strings, uniqueNumerics);
                }
                groupStats = new RunningStats(numColumns);
                longs = new long[rowGroupSize][];
                hasNulls = new boolean[numColumns];
                strings = new String[rowGroupSize][];
                uniqueNumerics = new TLongIntHashMap[numColumns];
                isNull = new boolean[rowGroupSize][];

                for (int i = 0; i < rowGroupSize; i++) {
                    longs[i] = new long[numColumns];
                    strings[i] = new String[numColumns];
                    isNull[i] = new boolean[numColumns];
                }

                for (int i = 0; i < numColumns; i++) {
                    uniqueNumerics[i] = new TLongIntHashMap();
                }
            }

            groupStats.addRow();
            docStats.addRow();

            for (int i = 0; i < numColumns; i++) {
                String value = row.getField(i);
                if (value == null)
                    throw new IllegalArgumentException();

                docStats.countUnique(i, value);
                groupStats.countUnique(i, value);
                if (value.isEmpty()) {
                    docStats.addNull(i);
                    groupStats.addNull(i);
                    isNull[groupRow][i] = true;
                    hasNulls[i] = true;
                    continue;
                }

                Map<String, Integer> enum2Id = (Map<String, Integer>) enum2Ids[i];

                Type type = schema.getFields().get(i).getType();
                if (type.equals(BooleanType.BOOLEAN)) {
                    byte b = 0;
                    if (CsvSchemaInferer.isYes(value))
                        b = 1;

                    docStats.addLong(i, b);
                    groupStats.addLong(i, b);
                    longs[groupRow][i] = b;
                } else if (type.equals(DateType.DATE)) {
                    long l = 1;
                    longs[groupRow][i] = l; // TODO
                    docStats.addLong(i, l);
                    groupStats.addLong(i, l);
                } else if (type.equals(TimestampType.TIMESTAMP)) {
                    long l = 2;
                    longs[groupRow][i] = l; // TODO
                    docStats.addLong(i, l);
                    groupStats.addLong(i, l);
                } else if (type.equals(TinyintType.TINYINT)) {
                    try {
                        int byteValue = Byte.parseByte(value);
                        longs[groupRow][i] = byteValue;
                        docStats.addLong(i, byteValue);
                        groupStats.addLong(i, byteValue);
                    } catch (NumberFormatException nfe) {
                        isNull[groupRow][i] = true;
                    }
                } else if (type.equals(SmallintType.SMALLINT)) {
                    try {
                        int shortValue = Short.parseShort(value);
                        longs[groupRow][i] = shortValue;
                        docStats.addLong(i, shortValue);
                        groupStats.addLong(i, shortValue);
                    } catch (NumberFormatException nfe) {
                        isNull[groupRow][i] = true;
                    }
                } else if (type.equals(IntegerType.INTEGER)) {
                    try {
                        int intValue = Integer.parseInt(value);
                        longs[groupRow][i] = intValue;
                        docStats.addLong(i, intValue);
                        groupStats.addLong(i, intValue);
                    } catch (NumberFormatException nfe) {
                        isNull[groupRow][i] = true;
                    }
                } else if (type.equals(BigintType.BIGINT)) {
                    try {
                        long l = Long.parseLong(value);
                        longs[groupRow][i] = l;
                        docStats.addLong(i, l);
                        groupStats.addLong(i, l);
                    } catch (NumberFormatException nfe) {
                        isNull[groupRow][i] = true;
                    }
                } else if (type.equals(RealType.REAL)) {
                    try {
                        float f = Float.parseFloat(value);
                        longs[groupRow][i] = Float.floatToRawIntBits(f);
                        docStats.addFloat(i, f);
                        groupStats.addFloat(i, f);
                    } catch (NumberFormatException nfe) {
                        isNull[groupRow][i] = true;
                    }
                } else if (type.equals(VarcharType.VARCHAR)) {
                    docStats.addString(i, value);
                    groupStats.addString(i, value);
                    if (enum2Id != null) {
                        // null gets its own id
                        int intValue = enum2Id.get(value).intValue();
                        longs[groupRow][i] = intValue;
                        docStats.addLong(i, intValue);
                        groupStats.addLong(i, intValue);
                    } else {
                        strings[groupRow][i] = value;
                    }
                } else {
                    throw new IllegalArgumentException("unexpected type: " + type);
                }

                if (strings[groupRow][i] != null)
                    continue;

                if (isNull[groupRow][i]) {
                    docStats.addNull(i);
                    groupStats.addNull(i);
                    hasNulls[i] = true;
                    continue;
                }

                // Accumulate unique numerics to see if we can do a dictionary
                // encoding
                if (!uniqueNumerics[i].contains(longs[groupRow][i])) {
                    uniqueNumerics[i].put(longs[groupRow][i], uniqueNumerics[i].size());
                }
            }

            row = parser.nextRow();
            currentRow++;
        }

        writeRowGroup(groupStats, hasNulls, isNull, longs, strings, uniqueNumerics);
    }

    private void writeRowGroup(
            RunningStats stats,
            boolean[] hasNulls,
            boolean[][] isNull,
            long[][] longs,
            String[][] strings,
            TLongIntHashMap[] uniqueNumerics
    ) throws IOException {
        int rows = stats.getRows();
        rowGroupOffsets.add(dos.size());
        // placeholders for uncompressed + compressed length so we can use
        // LZ4FastDecompressor
        dos.writeInt(0);
        dos.writeInt(0);
        baos.reset();
        rowGroupStats.add(stats);
        System.out.print("called on " + rows + " rows: ");
        boolean[] useDict = new boolean[numColumns];
        for (int i = 0; i < uniqueNumerics.length; i++) {
            int col = i;
            useDict[col] = uniqueNumerics[col].size() != 0 && uniqueNumerics[col].size() < 256;
            System.out.print(uniqueNumerics[i].size() + " ");
        }
        System.out.println();

        // pack fixed-length non-null fields first, put all strings at the end
        int[] columnOrder = new int[numColumns];
        {
            int j = 0;
            for (int i = 0; i < numColumns; i++) {
                if (!hasNulls[i] && isFixedLength(schema, i)) {
                    columnOrder[j] = i;
                    j++;
                }
            }

            for (int i = 0; i < numColumns; i++) {
                if (hasNulls[i] || !isFixedLength(schema, i)) {
                    columnOrder[j] = i;
                    j++;
                }
            }
        }

        DataOutputStream dos = new DataOutputStream(baos);
        ArrayList<Integer> rowOffsets = new ArrayList<>();

        for (int row = 0; row < rows; row++) {
            rowOffsets.add(dos.size());
            int numBytes = (int) Math.ceil(((double) numColumns) / 8.0);
            for (int i = 0; i < numBytes; i++) {
                byte b = 0;
                for (int j = i * 8; j < Math.min((i + 1) * 8, numColumns); j++) {
                    if (isNull[row][j])
                        b |= 1 << (j % 8);
                }
                dos.writeByte(b);

            }

            for (int col = 0; col < numColumns; col++) {
                if (isNull[row][col])
                    continue;

                if (useDict[col]) {
                    dos.writeByte(uniqueNumerics[col].get(longs[row][col]));
                    continue;
                }
                Type type = types[col];

                if (type.equals(VarcharType.VARCHAR)) {
                    // either: enum (so write a short) or string (so write bytes)
                    Map<String, Integer> enum2Id = (Map<String, Integer>) enum2Ids[col];
                    if (enum2Id == null) {
                        writeString(dos, strings[row][col]);
                    } else {
                        dos.writeShort((short) longs[row][col]);
                    }
                } else if (type.equals(RealType.REAL)) {
                    dos.writeFloat(Float.intBitsToFloat((int) longs[row][col]));
                } else if (type.equals(BooleanType.BOOLEAN)) {
                    dos.writeByte((byte) longs[row][col]);
                } else if (type.equals(IntegerType.INTEGER)) {
                    dos.writeInt((int) longs[row][col]);
                } else if (type.equals(BigintType.BIGINT)) {
                    dos.writeLong(longs[row][col]);
                } else if (type.equals(DateType.DATE)) {
                    dos.writeInt((int) longs[row][col]);
                } else if (type.equals(TimestampType.TIMESTAMP)) {
                    dos.writeLong(longs[row][col]);
                } else if (type.equals(SmallintType.SMALLINT)) {
                    dos.writeShort((short) longs[row][col]);
                } else if (type.equals(TinyintType.TINYINT)) {
                    dos.writeByte((byte) longs[row][col]);
                } else {
                    throw new IllegalArgumentException("unknown type: " + type);
                }

            }
        }

        dos.flush();

        ByteArrayOutputStream baosMeta = new ByteArrayOutputStream();
        DataOutputStream dosMeta = new DataOutputStream(baosMeta);
        dosMeta.writeInt(stats.getRows());
        for (int i = 0; i < rowOffsets.size(); i++) {
            dosMeta.writeInt(rowOffsets.get(i));
        }
        for (int i = 0; i < columnOrder.length; i++) {
            dosMeta.writeByte(columnOrder[i]);
        }

        for (int i = 0; i < uniqueNumerics.length; i++) {
            if (useDict[i]) {
                dosMeta.writeByte(uniqueNumerics[i].size());
                // uniqueNumerics[i] is long -> int
                // want int -> long, then sort by int
                long[] vals = new long[uniqueNumerics[i].size()];
                TLongIntIterator it = uniqueNumerics[i].iterator();
                while (it.hasNext()) {
                    it.advance();
                    vals[it.value()] = it.key();
                }
                for (int j = 0; j < vals.length; j++) {
                    dosMeta.writeLong(vals[j]);
                }

            } else {
                dosMeta.writeByte(0);
            }
        }

        dosMeta.flush();

        // compress row offsets, column orders
        byte[] toCompress = new byte[dosMeta.size() + dos.size()];
        byte[] baosMetaBytes = baosMeta.toByteArray();
        System.arraycopy(baosMetaBytes, 0, toCompress, 0, baosMetaBytes.length);
        byte[] baosBytes = baos.toByteArray();
        System.arraycopy(baosBytes, 0, toCompress, baosMetaBytes.length, baosBytes.length);
        int maxLength = lz4c.maxCompressedLength(toCompress.length);

        // Compressing an lz4 stream twice actually produces noticeable compression (~40%)
        // on the second run. See https://www.reddit.com/r/programming/comments/vyu7r/compressing_log_files_twice_improves_ratio/
        byte[] once = lz4c.compress(toCompress);
        byte[] twice = lz4c.compress(once);
        rowGroupRawLength.add(toCompress.length);
        rowGroupCompressedLength.add(once.length);
        this.dos.write(twice);
    }

    private boolean isFixedLength(CsvSchema schema, int i) {
        return schema.getEnumValues()[i] != null ||
                !schema.getFields().get(i).getType().equals(VarcharType.VARCHAR);
    }


}
