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

public class MmapRecord {
    public static final int MAX_FIELDS = 128;
    public final int[] offsets = new int[MAX_FIELDS];
    private final int[] emptyOffsets = new int[MAX_FIELDS];
    public final byte[] bytes;
    /* if true, need to adjust start/end offset by +1/-1 */
    public final boolean[] quoted = new boolean[MAX_FIELDS];
    /* if true, need to evaluate quoted characters inside the field; can't just hand
       to new String() */
    public final boolean[] containsQuotes = new boolean[MAX_FIELDS];

    public MmapRecord(byte[] bytes) {
        this.bytes = bytes;
        for(int i = 0; i < emptyOffsets.length; i++)
            emptyOffsets[i] = -1;

        reset();
    }

    public int getStart(int field) {
        int rv = offsets[field];
        if(field != 0)
            rv++;

        return rv;
    }

    public int getLength(int field) {
        final int end = offsets[field + 1];
        final int start = getStart(field);
        return end - start;
    }

    public int getNumFields() {
        for(int i = 0; i < offsets.length; i++)
            if(offsets[i] == -1)
                return i - 1;

        throw new IllegalArgumentException("cannot determine # of fields");
    }

    public void reset() {
        System.arraycopy(emptyOffsets, 0, offsets, 0, offsets.length);
    }
}