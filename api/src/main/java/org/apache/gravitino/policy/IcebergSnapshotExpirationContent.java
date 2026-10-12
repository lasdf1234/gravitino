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
package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.MetadataObject;

/**
 * Placeholder built-in policy content for Iceberg snapshot expiration.
 *
 * <p>Configurable thresholds will be added when the expire-snapshots policy lands; this class
 * registers the built-in type for TMS task mapping. TMS peer fields ({@code schedule}, {@code
 * minIntervalMs}, {@code jobOptions}, {@code updateOptions}) may be stored for schema symmetry;
 * generated {@code trigger-expr} remains {@code false} so peers are not evaluated for enqueue until
 * the type is fully wired.
 */
public class IcebergSnapshotExpirationContent extends TableMaintenanceContent {
  /** Property key for strategy type. */
  public static final String STRATEGY_TYPE_KEY = "strategy.type";
  /** Property key for the job template. */
  public static final String JOB_TEMPLATE_NAME_KEY = "job.template-name";
  /** Rule key for the trigger expression. */
  public static final String TRIGGER_EXPR_KEY = "trigger-expr";
  /** Rule key for the score expression. */
  public static final String SCORE_EXPR_KEY = "score-expr";
  /** Strategy type for snapshot expiration. */
  public static final String STRATEGY_TYPE_VALUE = "iceberg-snapshot-expiration";
  /** Built-in job template name for expire snapshots. */
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-expire-snapshots";

  private static final Set<MetadataObject.Type> SUPPORTED_OBJECT_TYPES =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);

  /** Default constructor for Jackson and {@link PolicyContents}; not a public API. */
  IcebergSnapshotExpirationContent() {}

  @Override
  public Set<MetadataObject.Type> supportedObjectTypes() {
    return SUPPORTED_OBJECT_TYPES;
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of(
        STRATEGY_TYPE_KEY, STRATEGY_TYPE_VALUE, JOB_TEMPLATE_NAME_KEY, JOB_TEMPLATE_NAME_VALUE);
  }

  @Override
  public Map<String, Object> rules() {
    Map<String, Object> rules = new LinkedHashMap<>();
    // Placeholders must not auto-trigger until real thresholds land in a follow-up.
    rules.put(TRIGGER_EXPR_KEY, "false");
    rules.put(SCORE_EXPR_KEY, "1");
    return Collections.unmodifiableMap(rules);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IcebergSnapshotExpirationContent)) {
      return false;
    }
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), IcebergSnapshotExpirationContent.class);
  }

  @Override
  public String toString() {
    return "IcebergSnapshotExpirationContent{"
        + "schedule="
        + schedule()
        + ", minIntervalMs="
        + minIntervalMs()
        + ", jobOptions="
        + jobOptions()
        + ", updateOptions="
        + updateOptions()
        + '}';
  }
}
