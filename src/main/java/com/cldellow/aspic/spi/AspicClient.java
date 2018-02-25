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

import com.cldellow.aspic.core.CsvSchema;
import com.cldellow.aspic.core.Json;
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class AspicClient {
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Map<String, Map<String, AspicTable>> schemas;

    @Inject
    public AspicClient(AspicConfig config, JsonCodec<Map<String, List<AspicTable>>> catalogCodec) {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        schemas = new HashMap<>();
        Vector<AspicColumn> columns = new Vector<>();
        columns.add(new AspicColumn("year", BigintType.BIGINT));

        HashMap<String, AspicTable> deflt = new HashMap<>();
        CsvSchema fs = fileStats("/tmp/tmphive/rent/big.csv.metadata");
         deflt.put("rent",
                new AspicTable(
                        "rent",
                        fs.getFields(),
                        null,
                        new Vector<Long>()));
        schemas.put("default", deflt);
        //Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
    }

    CsvSchema fileStats(String s) {
        try {
            return Json.FILE_STATS_CODEC.fromJson(Resources.toByteArray(
                    new URL("file:" + s)));

        } catch(MalformedURLException mue) {
            throw new RuntimeException(mue);
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    public Set<String> getSchemaNames() {
        return schemas.keySet();
    }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        Map<String, AspicTable> tables = schemas.get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public AspicTable getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, AspicTable> tables = schemas.get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }
}