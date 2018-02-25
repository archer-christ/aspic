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

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class CsvSchema {
    private final List<Field> fields;
    private final char fieldSeparator;
    private final int rows;
    private final String[][] enumValues;
    private boolean byteOrderMark;

    public CsvSchema(
            boolean byteOrderMark,
            List<Field> fields,
            char fieldSeparator,
            int rows,
            String[][] enumValues) {
        this.byteOrderMark = byteOrderMark;
        this.fields = fields;
        this.rows = rows;
        this.fieldSeparator = fieldSeparator;
        this.enumValues = enumValues;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rows + " rows <");
        for(int i = 0; i < fields.size(); i++) {
            if(i > 0)
                sb.append(",");
            if(enumValues[i] == null)
                sb.append(fields.get(i).getType());
            else {
                sb.append("enum[");
                sb.append(enumValues[i].length);
                sb.append("]");
            }
        }
        sb.append(">");
        return sb.toString();
    }

    public boolean getByteOrderMark() { return byteOrderMark; }
    public char getFieldSeparator() { return fieldSeparator; }
    public int getRows() { return rows; }
    public List<Field> getFields() { return fields; }
    public String[][] getEnumValues() { return enumValues; }

    public CsvSchema withFieldType(int i, Type type) {
        String[][] newEnumValues = new String[enumValues.length][];
        for(int j = 0; j < enumValues.length; j++)
            newEnumValues[j] = enumValues[j];

        newEnumValues[i] = null;

        Field[] newFields = fields.toArray(new Field[] {});
        newFields[i] = new Field(newFields[i].getName(), type);

        return new CsvSchema(byteOrderMark, Arrays.asList(newFields), fieldSeparator, rows, newEnumValues);
    }
}