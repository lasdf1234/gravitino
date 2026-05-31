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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

/**
 * Flink catalog table that combines Gravitino metadata with a native Flink {@link CatalogTable}.
 *
 * <p>Gravitino supplies schema, comment, and partition keys. The native table supplies connector
 * options and runtime-specific metadata such as Iceberg snapshot identifiers.
 */
public class FlinkDualCatalogTable implements CatalogTable {

  private final CatalogTable gravitinoTable;
  private final CatalogTable nativeTable;

  /**
   * Creates a dual catalog table from Gravitino metadata and a native catalog table.
   *
   * @param gravitinoTable Flink catalog table built from Gravitino metadata
   * @param nativeTable native Flink catalog table for runtime operations
   */
  public FlinkDualCatalogTable(CatalogTable gravitinoTable, CatalogTable nativeTable) {
    this.gravitinoTable = gravitinoTable;
    this.nativeTable = nativeTable;
  }

  /** Returns the Gravitino-backed Flink catalog table used for metadata operations. */
  public CatalogTable gravitinoTable() {
    return gravitinoTable;
  }

  /** Returns the native Flink catalog table used for runtime operations. */
  public CatalogTable nativeTable() {
    return nativeTable;
  }

  @Override
  public Schema getUnresolvedSchema() {
    return gravitinoTable.getUnresolvedSchema();
  }

  @Override
  public String getComment() {
    return gravitinoTable.getComment();
  }

  @Override
  public List<String> getPartitionKeys() {
    return gravitinoTable.getPartitionKeys();
  }

  @Override
  public boolean isPartitioned() {
    return gravitinoTable.isPartitioned();
  }

  @Override
  public Map<String, String> getOptions() {
    return nativeTable.getOptions();
  }

  @Override
  public Optional<Long> getSnapshot() {
    return nativeTable.getSnapshot();
  }

  @Override
  public TableKind getTableKind() {
    return TableKind.TABLE;
  }

  @Override
  public Optional<String> getDescription() {
    return gravitinoTable.getDescription();
  }

  @Override
  public Optional<String> getDetailedDescription() {
    return gravitinoTable.getDetailedDescription();
  }

  @Override
  public CatalogTable copy(Map<String, String> options) {
    return new FlinkDualCatalogTable(gravitinoTable, nativeTable.copy(options));
  }

  @Override
  public CatalogBaseTable copy() {
    return new FlinkDualCatalogTable(
        (CatalogTable) gravitinoTable.copy(), (CatalogTable) nativeTable.copy());
  }
}
