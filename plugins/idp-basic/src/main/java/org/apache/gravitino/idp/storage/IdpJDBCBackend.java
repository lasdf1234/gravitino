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
package org.apache.gravitino.idp.storage;

import static org.apache.gravitino.Configs.GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT;

import java.io.IOException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;

/**
 * JDBC backend for built-in IdP metadata. Extends core {@link JDBCBackend} and routes IdP entity
 * garbage collection to the IdP SPI services.
 */
public class IdpJDBCBackend extends JDBCBackend {

  @Override
  public int hardDeleteLegacyData(Entity.EntityType entityType, long legacyTimeline)
      throws IOException {
    switch (entityType) {
      case IDP_USER:
        return IdpUserMetaService.getInstance()
            .deleteUserMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case IDP_GROUP:
        return IdpGroupMetaService.getInstance()
            .deleteGroupMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      default:
        return super.hardDeleteLegacyData(entityType, legacyTimeline);
    }
  }

  @Override
  public int deleteOldVersionData(Entity.EntityType entityType, long versionRetentionCount)
      throws IOException {
    switch (entityType) {
      case IDP_USER:
      case IDP_GROUP:
        return 0;
      default:
        return super.deleteOldVersionData(entityType, versionRetentionCount);
    }
  }
}
