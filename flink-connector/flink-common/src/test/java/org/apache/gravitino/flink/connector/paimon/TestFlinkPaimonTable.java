/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.flink.connector.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.paimon.flink.DataCatalogTable;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;

class TestFlinkPaimonTable {

  @Test
  void testMetadataFromGravitinoTable() {
    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
    CatalogTable gravitinoTable =
        CatalogTable.of(schema, "comment", Collections.singletonList("dt"), Collections.emptyMap());
    DataCatalogTable nativeTable = mock(DataCatalogTable.class);
    FileStoreTable paimonTable = mock(FileStoreTable.class);
    when(nativeTable.table()).thenReturn(paimonTable);
    when(nativeTable.getOptions()).thenReturn(Collections.emptyMap());

    FlinkPaimonTable flinkPaimonTable = new FlinkPaimonTable(gravitinoTable, nativeTable);

    assertSame(gravitinoTable, flinkPaimonTable.gravitinoTable());
    assertEquals(schema, flinkPaimonTable.getUnresolvedSchema());
    assertEquals("comment", flinkPaimonTable.getComment());
    assertEquals(Collections.singletonList("dt"), flinkPaimonTable.getPartitionKeys());
    assertSame(paimonTable, flinkPaimonTable.table());
  }
}
