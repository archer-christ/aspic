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

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AspicSplitManager
        implements ConnectorSplitManager {
    private final String connectorId;
    private final AspicClient exampleClient;

    @Inject
    public AspicSplitManager(AspicConnectorId connectorId, AspicClient exampleClient) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.exampleClient = requireNonNull(exampleClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy) {
        AspicTableLayoutHandle layoutHandle = (AspicTableLayoutHandle) layout;
        AspicTableHandle tableHandle = layoutHandle.getTable();
        AspicTable table = exampleClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();

        for (int i = 0; i < table.getRowGroupOffsets().size(); i++) {
            long start = table.getRowGroupOffsets().get(i);

            long end = start;

            if (i != table.getRowGroupOffsets().size() - 1) {
                end = table.getRowGroupOffsets().get(i+1);
            } else {
                end = table.getLength();
            }
            splits.add(new AspicSplit(
                    connectorId,
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    table.getFile(),
                    start,
                    end));
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}