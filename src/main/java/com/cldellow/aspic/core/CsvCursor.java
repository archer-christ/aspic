package com.cldellow.aspic.core;

import java.nio.ByteBuffer;

/** Has no concept of types, for that, see {@link com.cldellow.aspic.spi.AspicRecordCursor}.
 *  Needs to be configured appropriately, for code to infer types, see {@link CsvSchemaInferer}.
 */
public class CsvCursor {
    private final ByteBuffer buffer;
    private final int end;
    private final boolean unixNewline;
    private final byte[] bytes;
    private final MmapRecord record;
    private int pos;
    private int bufferIndex;
    private int bufferLength;

    public CsvCursor(ByteBuffer buffer,
                     MmapRecord record,
                     int end,
                     boolean unixNewline) {
        this.buffer = buffer;
        this.end = end;
        this.unixNewline = unixNewline;
        this.record = record;
        this.bytes = record.bytes;
        pos = buffer.position();
    }

    public boolean next() {
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
            if (!unixNewline)
                record.offsets[field]--;
            pos++;
            bufferIndex++;
        }

        return true;
    }

    public int getPos() { return pos; }
}
