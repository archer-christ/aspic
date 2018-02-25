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

import com.cldellow.aspic.core.Field;
import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class AspicTable {
    private final String name;
    private final List<ColumnMetadata> columnsMetadata;
    private final String file;
    private final List<Long> rowGroupOffsets;

    public AspicTable(
            String name,
            List<Field> fields,
            String file,
            List<Long> rowGroupOffsets) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.file = requireNonNull(file, "file is null");
        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (Field field : fields) {
            columnsMetadata.add(new ColumnMetadata(field.getName(), field.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
        this.rowGroupOffsets = rowGroupOffsets;
    }

    public long getLength() {
        File f = new File(file);
        return f.length();
    }

    public String getName() {
        return name;
    }

    public String getFile() {
        return file;
    }

    public List<Long> getRowGroupOffsets() { return rowGroupOffsets; }

    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }
}