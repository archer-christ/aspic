package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.Type;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class AspicRowGroup {
    private final long[][] columnDicts;
    private final int numRows;
    private final ByteBuffer buffer;
    private final int[] rowOffsets;
    private final int rowStart;
    private final int nullByteSize;

    // < 0 => you need to pointer chase to find the position in memory
    private final int[] columnOffsets;
    private final int[] columnWidths;
    private int currentRowStart = -1;
    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final String[][] enumValues;
    private final boolean[] isNull;
    private final int numColumns;

    public AspicRowGroup(
            String file,
            String[][] enumValues,
            Type[] types,
            int offset,
            int length) throws FileNotFoundException, IOException {
        numColumns = types.length;
//        System.out.println("offset=" + offset + ", length=" + length);
        this.enumValues = enumValues;
        nullByteSize = (int) Math.ceil(((double) numColumns) / 8.0);
        isNull = new boolean[numColumns];
        byte[] raw;
        {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            FileChannel channel = raf.getChannel();
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
            int rawLength = buffer.getInt();
            int compressedOnceLength = buffer.getInt();
            byte[] compressed = new byte[length - 8];
            buffer.get(compressed);
            LZ4FastDecompressor decompressor = AspicWriter.factory.fastDecompressor();
            byte[] once = new byte[compressedOnceLength];
            decompressor.decompress(compressed, once);
            raw = new byte[rawLength];
            decompressor.decompress(once, raw);

            channel.close();
            raf.close();
        }

        buffer = ByteBuffer.wrap(raw);
        columnDicts = new long[numColumns][];
        numRows = buffer.getInt();
        rowOffsets = new int[numRows];
        for (int i = 0; i < numRows; i++) {
            rowOffsets[i] = buffer.getInt();
        }
        // an optimized column order is here. it's ignored for now.
        for (int i = 0; i < numColumns; i++)
            buffer.get();
        for (int i = 0; i < numColumns; i++) {
            int size = byteAsUnsigned(buffer.get());
//            System.out.println("dict size for column " + i + " is " + size);
            if (size > 0) {
                columnDicts[i] = new long[size];
                for (int j = 0; j < size; j++) {
                    columnDicts[i][j] = buffer.getLong();
                }
            }
        }

        columnWidths = new int[numColumns];
        columnOffsets = new int[numColumns];
        for (int i = 0; i < numColumns; i++) {
            int width = -1;

            if (columnDicts[i] != null) {
                // a dict is a 1 byte lookup
                width = 1;
            } else if (enumValues[i] != null) {
                // an enum is a 2 byte lookup
                width = 2;
            } else {
                width = TypeSerializer.width(types[i]);
            }

            columnWidths[i] = width;

            // if offset of previous entry is unknown, so is this one
            if (i == 0) {
                // 0th offset is trivially known
            } else if (columnOffsets[i - 1] == -1 || columnWidths[i - 1] == -1) {
                // if previous offset isn't fixed, or previous width
                // is variable, neither is this one
                columnOffsets[i] = -1;
            } else {
                columnOffsets[i] = columnOffsets[i - 1] + columnWidths[i - 1];
            }
        }

        rowStart = buffer.position();
//        System.out.println("numRows=" + numRows);
    }

    private static int byteAsUnsigned(byte a) {
        int b = a & 0xFF;
        return b;
    }


    public int getNumRows() { return numRows; }
    public void setRow(int row) {
//        System.out.println(rowOffsets[row]);
        currentRowStart = rowStart + rowOffsets[row] + nullByteSize;
        buffer.position(currentRowStart - nullByteSize);
        int nulls = 0;
        for(int i = 0; i < nullByteSize; i++) {
            final byte b = buffer.get();
            final int to = Math.min(numColumns, (i + 1) * 8);
            for(int j = i * 8; j < numColumns && j < to; j++) {
                isNull[j] = 1 == ((b >> (j % 8)) & 1);
            }
        }
//        System.out.println("setting target to: " + target);
    }

    public float getFloat(int col) {
        int width = columnWidths[col];
        int offset = columnOffsets[col];
        if (width == -1 || offset == -1) {
            throw new IllegalArgumentException("cannot handle column=" + col + ", width=" + width + ", offset=" + offset);
        }

        buffer.position(currentRowStart + columnOffsets[col]);
        if (columnDicts[col] != null) {
            int idx = byteAsUnsigned(buffer.get());
            return Float.intBitsToFloat((int)columnDicts[col][idx]);
        }

        return buffer.getFloat();
    }

    public String getString(int col) {
        int width = columnWidths[col];
        int offset = columnOffsets[col];
        if (width == -1 || offset == -1) {
            throw new IllegalArgumentException("cannot handle column=" + col + ", width=" + width + ", offset=" + offset);
        }

        buffer.position(currentRowStart + columnOffsets[col]);
        if(enumValues[col] == null) {
            short len = buffer.getShort();
            byte[] bytes = new byte[len];
            buffer.get(bytes);
            return new String(bytes, UTF8);
        }

        if (columnDicts[col] != null) {
            int idx = byteAsUnsigned(buffer.get());
            return enumValues[col][(int)columnDicts[col][idx]];
        }

        int idx = buffer.getShort();
        return enumValues[col][idx];
    }

    public long getLong(int col) {
        int width = columnWidths[col];
        int offset = columnOffsets[col];
        if (width == -1 || offset == -1) {
            throw new IllegalArgumentException("cannot handle column=" + col + ", width=" + width + ", offset=" + offset);
        }

        buffer.position(currentRowStart + columnOffsets[col]);
        if (columnDicts[col] != null) {
            int idx = byteAsUnsigned(buffer.get());
            return columnDicts[col][idx];
        }

        if(width == 1)
            return buffer.get();
        if(width == 2)
            return buffer.getShort();
        if(width == 4)
            return buffer.getInt();
        if(width == 8)
            return buffer.getLong();

        throw new IllegalArgumentException("unexpected");
    }
}
