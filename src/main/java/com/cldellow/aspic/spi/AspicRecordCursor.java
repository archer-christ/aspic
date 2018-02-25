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
package com.cldellow.aspic.spi;

import com.cldellow.aspic.core.CsvCursor;
import com.cldellow.aspic.core.MmapRecord;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;

public class AspicRecordCursor
        implements RecordCursor {
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<AspicColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final long start;
    private final long end;
    private final String file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final byte[] bytes = new byte[65536];
    private final MmapRecord record = new MmapRecord(bytes);
    private final ByteBuffer buffer;
    private long pos;

    private final CsvCursor cursor;

    public AspicRecordCursor(List<AspicColumnHandle> columnHandles,
                             String file,
                             long start,
                             long end) {
        try {
            this.columnHandles = columnHandles;

            fieldToColumnIndex = new int[columnHandles.size()];
            for (int i = 0; i < columnHandles.size(); i++) {
                AspicColumnHandle columnHandle = columnHandles.get(i);
                fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
            }

            this.file = file;
            this.start = start;
            pos = start;
            this.end = end;
            this.raf = new RandomAccessFile(file, "r");
            this.channel = raf.getChannel();
            this.buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, end);
            buffer.position((int) start);
            cursor = new CsvCursor(buffer, record, (int)end, true);
//            System.out.println("AspicRecordCursor start=" + start + ", end=" + end);
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException(fnfe);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public long getCompletedBytes() {
        // TODO: confirm if pos is relative to start
        return pos - start;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        if(cursor.getPos() >= end)
            return false;

        cursor.next();
        return true;
    }

    private String getFieldValue(int index) {
        int field = fieldToColumnIndex[index];
        String rv = new String(record.bytes, record.getStart(field), record.getLength(field));
        return rv;
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT);
        int start = record.getStart(field);
        int len = record.getLength(field);
        int scale = 1;
        int rv = 0;
        for (int i = start; i < start + len; i++) {
            if (i == start && record.bytes[i] == '-') {
                scale = -1;
            } else {
                rv = 10 * rv + record.bytes[i] - '0';
            }
        }
        return scale * rv;
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");

        int len = record.getLength(field);
        if (len == 0)
            return true;

        if (getType(field) == BigintType.BIGINT) {
            int start = record.getStart(field);
            for(int i = start; i < start + len; i++) {
                if(i == start && record.bytes[i] == '-')
                    continue;

                if(record.bytes[i] >= '0'  && record.bytes[i] <= '9')
                    continue;

                return true;
            }
        }

        return false;
    }

    private void checkFieldType(int field, Type expected) {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close() {
        try {
            channel.close();
            raf.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }

    }
}