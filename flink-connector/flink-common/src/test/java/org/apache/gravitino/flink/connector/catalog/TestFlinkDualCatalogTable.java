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

package org.apache.gravitino.flink.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.gravitino.flink.connector.hive.FlinkHiveTable;
import org.apache.gravitino.flink.connector.iceberg.FlinkIcebergTable;
import org.apache.gravitino.flink.connector.jdbc.FlinkJdbcTable;
import org.junit.jupiter.api.Test;

class TestFlinkDualCatalogTable {

  @Test
  void testMetadataFromGravitinoAndOptionsFromNative() {
    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
    CatalogTable gravitinoTable =
        CatalogTable.of(
            schema, "gravitino comment", Collections.singletonList("dt"), ImmutableMap.of());
    CatalogTable nativeTable =
        CatalogTable.of(
            Schema.newBuilder().column("ignored", DataTypes.STRING()).build(),
            "native comment",
            Collections.emptyList(),
            ImmutableMap.of("connector", "iceberg", "catalog-name", "native"));

    FlinkDualCatalogTable dualTable = new FlinkDualCatalogTable(gravitinoTable, nativeTable);

    assertSame(gravitinoTable, dualTable.gravitinoTable());
    assertSame(nativeTable, dualTable.nativeTable());
    assertEquals(schema, dualTable.getUnresolvedSchema());
    assertEquals("gravitino comment", dualTable.getComment());
    assertEquals(Collections.singletonList("dt"), dualTable.getPartitionKeys());
    assertEquals(
        ImmutableMap.of("connector", "iceberg", "catalog-name", "native"), dualTable.getOptions());
  }

  @Test
  void testSnapshotFromNative() {
    CatalogTable gravitinoTable =
        CatalogTable.of(
            Schema.newBuilder().build(), "", Collections.emptyList(), ImmutableMap.of());
    CatalogTable nativeTable =
        CatalogTable.of(
            Schema.newBuilder().build(), "", Collections.emptyList(), ImmutableMap.of(), 42L);

    FlinkDualCatalogTable dualTable = new FlinkDualCatalogTable(gravitinoTable, nativeTable);

    assertEquals(Optional.of(42L), dualTable.getSnapshot());
  }

  @Test
  void testConnectorSpecificSubclasses() {
    CatalogTable gravitinoTable =
        CatalogTable.of(
            Schema.newBuilder().column("id", DataTypes.INT()).build(),
            "c",
            Collections.emptyList(),
            ImmutableMap.of());
    CatalogTable nativeTable =
        CatalogTable.of(
            Schema.newBuilder().build(), "n", Collections.emptyList(), ImmutableMap.of("k", "v"));

    assertEquals("c", new FlinkIcebergTable(gravitinoTable, nativeTable).getComment());
    assertEquals(
        ImmutableMap.of("k", "v"), new FlinkJdbcTable(gravitinoTable, nativeTable).getOptions());
    assertEquals("c", new FlinkHiveTable(gravitinoTable, nativeTable).getComment());
  }
}
