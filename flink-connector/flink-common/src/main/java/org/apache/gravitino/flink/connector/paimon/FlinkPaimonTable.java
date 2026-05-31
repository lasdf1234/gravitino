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

import java.util.Collections;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.paimon.flink.DataCatalogTable;

/**
 * Flink Paimon table that combines Gravitino metadata with a native Paimon {@link
 * DataCatalogTable}, mirroring Spark {@code SparkPaimonTable}.
 *
 * <p>Gravitino supplies schema, comment, and partition keys for catalog operations and DDL. The
 * embedded native {@link org.apache.paimon.table.Table} retains {@code CatalogEnvironment} required
 * for Paimon runtime writes.
 */
public class FlinkPaimonTable extends DataCatalogTable {

  private final CatalogTable gravitinoTable;

  /**
   * Creates a Flink Paimon table from Gravitino metadata and a native Paimon catalog table.
   *
   * @param gravitinoTable Flink catalog table built from Gravitino metadata
   * @param nativeTable native Paimon {@link DataCatalogTable} for runtime operations
   */
  public FlinkPaimonTable(CatalogTable gravitinoTable, DataCatalogTable nativeTable) {
    super(
        nativeTable.table(),
        gravitinoTable.getUnresolvedSchema(),
        gravitinoTable.getPartitionKeys(),
        nativeTable.getOptions(),
        gravitinoTable.getComment(),
        Collections.emptyMap());
    this.gravitinoTable = gravitinoTable;
  }

  /** Returns the Gravitino-backed Flink catalog table used for metadata operations. */
  public CatalogTable gravitinoTable() {
    return gravitinoTable;
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
}
