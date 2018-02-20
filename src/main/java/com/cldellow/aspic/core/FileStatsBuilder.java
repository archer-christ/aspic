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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Vector;

public class FileStatsBuilder {
    public static final int DEFAULT_ROW_GROUP_SIZE = 100000;
    public final FileStats stats;
    final byte[] bytes = new byte[65536];
    final ByteBuffer buffer;
    final MmapRecord record = new MmapRecord(bytes);
    final long end;
    int bufferIndex = 0;
    int bufferLength = 0;
    long pos = 0L;

    FileStatsBuilder(String fileName) {
        this(fileName, sanitize(fileName), DEFAULT_ROW_GROUP_SIZE);
    }

    FileStatsBuilder(String fileName, String tableName, int rowGroupSize) {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r");
             FileChannel channel = raf.getChannel();
        ) {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());
            end = raf.length();
            // Do a pass over the file, building:
            //   offset of every Nth row start
            //   # of columns per row
            //   for each column, # of entries that can be interpreted as type X (will be used for inference)
            //   for each column, # of NULL entries
            //   for each group of N rows, stats

            int row = 0;
            String[] fieldNames = null;
            Vector<Long> rowGroupOffsets = new Vector<>();
            while (next()) {
                if (fieldNames == null) {
                    fieldNames = new String[record.getNumFields()];
                    for (int i = 0; i < fieldNames.length; i++) {
//                        System.out.println("i=" + i + ", start=" + record.getStart(i) + ", len=" + record.getLength(i));
                        fieldNames[i] = new String(record.bytes, record.getStart(i), record.getLength(i), Charset.forName("UTF-8"));
                    }

                    continue;
                }

                // pos points at \n that terminates this row
                if(row % rowGroupSize == 0) {
                    long rowStart = pos;
                    int rowEnd = record.offsets[0];
                    for (int i = 0; i < record.offsets.length; i++)
                        if (record.offsets[i] == -1)
                            break;
                        else
                            rowEnd = record.offsets[i];
                    rowStart -= rowEnd - record.offsets[0];
                    rowStart--;
//                    System.out.println("group " + (row/rowGroupSize) + " = " + rowStart);
                    rowGroupOffsets.add(rowStart);
                }
                row++;
            }

            Vector<Field> fields = new Vector<>();
            for(int i = 0; i < fieldNames.length; i++) {
                Field field = new Field(fieldNames[i], VarcharType.VARCHAR);
                if(fieldNames[i].equals("Ref_Date"))
                    field = new Field(fieldNames[i], BigintType.BIGINT);
                fields.add(field);
            }

            stats = new FileStats(fileName, tableName, ImmutableList.copyOf(fields), rowGroupOffsets, row);
        } catch (FileNotFoundException fnfe) {
            throw new RuntimeException(fnfe);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private boolean next() {
        if (pos >= end)
            return false;

        record.reset();

        // check if we need more data
        if (bufferIndex == bufferLength) {
            int toConsume = Math.min((int) (end - pos), bytes.length);
            //System.out.println("toConsume 1: " + toConsume);
            buffer.get(bytes, 0, toConsume);
            bufferLength = toConsume;
            bufferIndex = 0;
        }

        int startBufferIndex = bufferIndex;
        record.offsets[0] = bufferIndex;
        int field = 1;

        while (bufferIndex < bufferLength && bytes[bufferIndex] != '\n') {
            if (bytes[bufferIndex] == ',') {
                record.offsets[field] = bufferIndex;
                field++;
            }

            bufferIndex++;
            pos++;

            if (bufferIndex == bufferLength) {
                // Preserve the parts we've parsed from this row.
                int preservedLength = bufferLength - startBufferIndex;

                System.arraycopy(bytes, startBufferIndex, bytes, 0, preservedLength);

                int toConsume = Math.min((int) (end - pos), bytes.length - preservedLength);
                buffer.get(bytes, preservedLength, toConsume);
                bufferLength = preservedLength + toConsume;
                bufferIndex = preservedLength;

                for (int i = 0; i < field; i++)
                    record.offsets[i] -= startBufferIndex;
            }
        }

        if (bufferIndex < bufferLength && bytes[bufferIndex] == '\n') {
            record.offsets[field] = bufferIndex;
            pos++;
            bufferIndex++;
        }

        return true;
    }

    static String sanitize(String s) {
        if(s.contains("/")) {
            s = s.substring(s.lastIndexOf("/") + 1);
        }

        s = s.replace(".csv", "");

        return s.replaceAll("[^a-zA-Z0-9_]", "");
    }
}