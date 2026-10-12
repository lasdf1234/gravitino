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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.policy.MaintenanceTrigger;

/**
 * Base for the four built-in table-maintenance policy contents.
 *
 * <p>Holds optional TMS peer fields shared by all maintenance tasks: {@code schedule}, {@code
 * minIntervalMs}, {@code jobOptions}, and {@code updateOptions}. Task-specific thresholds stay on
 * subclasses. These peer fields are not constructor parameters and are not validated by subclass
 * {@code validate()} — validation is done by {@link
 * org.apache.gravitino.maintenance.policy.TableMaintenancePolicies}.
 *
 * <p>{@code jobOptions} and {@code updateOptions} are sibling runtime option bags for TMS job
 * submission. Cluster-static client auth is configured separately in {@code gravitino.conf} (see
 * {@code gravitino.maintenance.gravitinoAuth.*} and {@code gravitino.maintenance.ircAuth.*}). Empty
 * maps mean “use cluster defaults only”.
 */
public abstract class TableMaintenanceContent implements PolicyContent {

  @Nullable private MaintenanceTrigger schedule;
  @Nullable private Long minIntervalMs;
  private Map<String, String> jobOptions = Collections.emptyMap();
  private Map<String, String> updateOptions = Collections.emptyMap();

  /** Creates content with unset TMS peer fields. */
  protected TableMaintenanceContent() {}

  /**
   * @return evaluate trigger, or {@code null} when unset
   */
  @Nullable
  public MaintenanceTrigger schedule() {
    return schedule;
  }

  /**
   * @return cooldown after last finished job for this policy type, or {@code null}
   */
  @Nullable
  public Long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * @return Spark / IRC client properties for {@code runJob}; never {@code null}
   */
  public Map<String, String> jobOptions() {
    return jobOptions == null ? Collections.emptyMap() : jobOptions;
  }

  /**
   * @return Gravitino updater / control-plane options for {@code runJob}; never {@code null}
   */
  public Map<String, String> updateOptions() {
    return updateOptions == null ? Collections.emptyMap() : updateOptions;
  }

  /**
   * Attaches optional TMS peer fields after construction (used by {@link PolicyContents} / DTO
   * converters). Not part of subclass constructors.
   *
   * @param schedule evaluate trigger, or null
   * @param minIntervalMs cooldown ms, or null
   * @param jobOptions Spark / IRC options, or null
   * @param updateOptions updater options, or null
   */
  final void setPeerFields(
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    this.schedule = schedule;
    this.minIntervalMs = minIntervalMs;
    this.jobOptions = copyOptions(jobOptions);
    this.updateOptions = copyOptions(updateOptions);
  }

  /**
   * Whether any TMS peer field is set.
   *
   * @return {@code true} when schedule, interval, or either options map is present
   */
  public final boolean hasPeerFields() {
    return schedule != null
        || minIntervalMs != null
        || !jobOptions().isEmpty()
        || !updateOptions().isEmpty();
  }

  /**
   * Invokes {@link PolicyContent} default validation for subclasses that extend this base.
   *
   * @throws IllegalArgumentException when default content validation fails
   */
  protected final void validatePolicyContentDefaults() {
    PolicyContent.super.validate();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TableMaintenanceContent)) {
      return false;
    }
    TableMaintenanceContent that = (TableMaintenanceContent) o;
    return Objects.equals(schedule, that.schedule)
        && Objects.equals(minIntervalMs, that.minIntervalMs)
        && Objects.equals(jobOptions(), that.jobOptions())
        && Objects.equals(updateOptions(), that.updateOptions());
  }

  @Override
  public int hashCode() {
    return Objects.hash(schedule, minIntervalMs, jobOptions(), updateOptions());
  }

  private static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
    if (options == null || options.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(options));
  }
}
