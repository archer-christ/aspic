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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class FileStats {
    private final String file;
    private final String name;
    private final List<Field> fields;
    private final int rows;
    private final List<Long> rowGroupOffsets;
    private final String lineSeparator;

    @JsonCreator
    public FileStats(
            @JsonProperty("file") String file,
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<Field> fields,
            @JsonProperty("rowGroupOffsets") List<Long> rowGroupOffsets,
            @JsonProperty("rows") int rows,
            @JsonProperty("lineSeparator") String lineSeparator) {
        this.file = file;
        this.name = name;
        this.fields = fields;
        this.rows = rows;
        this.rowGroupOffsets = rowGroupOffsets;
        this.lineSeparator = lineSeparator;
    }


    @JsonProperty
    public String getFile() { return file; }

    @JsonProperty
    public String getName() { return name; }

    @JsonProperty
    public String getLineSeparator() { return lineSeparator; }

    @JsonProperty
    public int getRows() { return rows; }

    @JsonProperty
    public List<Field> getFields() { return fields; }

    @JsonProperty
    public List<Long> getRowGroupOffsets() { return rowGroupOffsets; }
}