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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.List;
import java.util.Vector;

import static java.util.Objects.requireNonNull;

public class AspicSplit
        implements ConnectorSplit {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String lineSeparator;
    private final String file;
    private final long start;
    private final long end;
    private final boolean remotelyAccessible;
    private final List<HostAddress> addresses;

    @JsonCreator
    public AspicSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("lineSeparator") String lineSeparator,
            @JsonProperty("file") String file,
            @JsonProperty("start") long start,
            @JsonProperty("end") long end) {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.file = requireNonNull(file, "file is null");
        this.lineSeparator = requireNonNull(lineSeparator, "lineSeparator is null");
        this.start = start;
        this.end = end;

//        if ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) {
        remotelyAccessible = true;
//        addresses = ImmutableList.of(HostAddress.fromUri(uri));
        addresses = new Vector<>();
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getFile() {
        return file;
    }

    @JsonProperty
    public long getStart() { return start; }

    @JsonProperty
    public long getEnd() { return end; }

    @JsonProperty
    public String getLineSeparator() { return lineSeparator; }

    @Override
    public boolean isRemotelyAccessible() {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }
}