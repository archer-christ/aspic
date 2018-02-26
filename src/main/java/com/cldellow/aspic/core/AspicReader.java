package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class AspicReader {
    private final int numColumns;
    private final Type[] types;
    private final String[] columnNames;
    private final String[][] enumValues;
    private final Charset UTF8 = Charset.forName("UTF-8");
    private final int[] rowGroupOffsets;
    private final int[] rowGroupLengths;
    private final String file;

    // To minimize the # of objects floating around - there are # of rowgroups + 1 * numColumns
    // elements in each array. Most will be null or zero.
    private final int[] numRows;
    private final long[] minLong;
    private final long[] maxLong;
    private final float[] minFloat;
    private final float[] maxFloat;
    private final String[] minString;
    private final String[] maxString;
    private final int[] numNulls;
    private final int[] numUniques;

    public AspicReader(String file) throws FileNotFoundException, IOException {
        this.file = file;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());

            // TODO: validate magic number = version

            /* Read preamble */
            buffer.position(5);
            int metadataStart = buffer.getInt();

            numColumns = buffer.get();
            types = new Type[numColumns];
            for (int i = 0; i < types.length; i++) {
                types[i] = TypeSerializer.idToType(buffer.get());
            }

            columnNames = new String[numColumns];
            for (int i = 0; i < columnNames.length; i++) {
                columnNames[i] = readString(buffer);
            }

            enumValues = new String[numColumns][];
            for (int i = 0; i < numColumns; i++) {
                short numValues = buffer.getShort();
                if (numValues > 0) {
                    enumValues[i] = new String[numValues];
                    for (int j = 0; j < numValues; j++) {
                        enumValues[i][j] = readString(buffer);
                    }
                }
            }

            buffer.position(metadataStart);
            final int numberRowGroups = buffer.getInt();
            /* Read row group stats, offsets. */
            rowGroupOffsets = new int[numberRowGroups];
            rowGroupLengths = new int[numberRowGroups];
            for (int i = 0; i < numberRowGroups; i++) {
                rowGroupOffsets[i] = buffer.getInt();
                if(i >= 1)
                    rowGroupLengths[i - 1] = rowGroupOffsets[i] - rowGroupOffsets[i - 1];
            }
            rowGroupLengths[numberRowGroups - 1] = metadataStart - rowGroupOffsets[numberRowGroups - 1];

            // doc stats, then stats for each rowgroup
            final int numEls = (1 + numberRowGroups) * numColumns;
            numRows = new int[1 + numberRowGroups];
            minLong = new long[numEls];
            maxLong = new long[numEls];
            minFloat = new float[numEls];
            maxFloat = new float[numEls];
            minString = new String[numEls];
            maxString = new String[numEls];
            numNulls = new int[numEls];
            numUniques = new int[numEls];
            readStats(buffer, numberRowGroups);
            for (int i = 0; i < numberRowGroups; i++)
                readStats(buffer, i);
        }
    }

    public String getFile() { return file; }
    public int getNumRowGroups() { return rowGroupOffsets.length; }
    public int getRowGroupOffset(int rowGroup) { return rowGroupOffsets[rowGroup]; }
    public int getRowGroupLength(int rowGroup) { return rowGroupLengths[rowGroup]; }

    public String[][] getEnumValues() { return enumValues; }
    public Type[] getTypes() { return types; }

    public void debug() {
        for (int i = 0; i < numColumns; i++) {
            int offset = rowGroupOffsets.length * numColumns + i;
            System.out.print(columnNames[i] + ": ");
            System.out.print("nulls=" + numNulls[offset]);
            System.out.print(", uniques=" + numUniques[offset]);
            if (types[i].equals(VarcharType.VARCHAR)) {
                System.out.print(", min=" + minString[offset]);
                System.out.print(", max=" + maxString[offset]);
            } else if (types[i].equals(RealType.REAL)) {
                System.out.print(", min=" + minFloat[offset]);
                System.out.print(", max=" + maxFloat[offset]);
            } else {
                System.out.print(", min=" + minLong[offset]);
                System.out.print(", max=" + maxLong[offset]);
            }
            System.out.println();
        }
    }

    private void readStats(ByteBuffer buffer, int index) {
        final int offset = index * numColumns;
        numRows[index] = buffer.getInt();
        final int ignored = buffer.getShort();
        final int numStrings = buffer.getShort();
        final String[] strs = new String[numStrings];
        for (int i = 0; i < numStrings; i++) {
            strs[i] = readString(buffer);
        }


        for (int i = 0; i < numColumns; i++) {
            numNulls[offset + i] = buffer.getInt();
            numUniques[offset + i] = buffer.getInt();
            long min = buffer.getLong();
            long max = buffer.getLong();

            if (types[i].equals(VarcharType.VARCHAR)) {
                minString[offset + i] = strs[(int) min];
                maxString[offset + i] = strs[(int) max];
            } else if (types[i].equals(RealType.REAL)) {
                minFloat[offset + i] = Float.intBitsToFloat((int) min);
                maxFloat[offset + i] = Float.intBitsToFloat((int) max);
            } else {
                minLong[offset + i] = min;
                maxLong[offset + i] = max;
            }
        }

    }

    private String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, UTF8);
    }
}
