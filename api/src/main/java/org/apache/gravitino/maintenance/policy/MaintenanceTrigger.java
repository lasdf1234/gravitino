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
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Evaluate triggers for table maintenance: event-based {@code onCommit} and/or time-based {@code
 * cronExpression}.
 *
 * <p>JSON property name on policy content remains {@code schedule} for design compatibility; this
 * type is named trigger because it covers both cron and commit events.
 *
 * <p>{@code cronExpression} is a <strong>Quartz cron</strong> expression with either 6 fields
 * ({@code seconds minutes hours day-of-month month day-of-week}) or 7 fields (optional trailing
 * {@code year}). Supported tokens per field: digits, {@code *}, {@code ,}, {@code -}, {@code /},
 * {@code ?}, {@code L}, {@code W}, {@code #}, and month/day-of-week names. Time zone is the server
 * default; this type does not carry a zone id.
 */
public final class MaintenanceTrigger {

  private static final Pattern CRON_FIELD =
      Pattern.compile("[0-9A-Za-z*/,?#\\-LW]+", Pattern.CASE_INSENSITIVE);

  private final boolean onCommit;
  @Nullable private final String cronExpression;

  /** Default constructor for Jackson field-based deserialization only. */
  private MaintenanceTrigger() {
    this(false, null);
  }

  /**
   * Creates a maintenance trigger.
   *
   * @param onCommit whether IRC commit should enqueue maintenance
   * @param cronExpression Quartz cron expression, or null / blank when unset
   */
  public MaintenanceTrigger(Boolean onCommit, @Nullable String cronExpression) {
    this.onCommit = onCommit != null && onCommit;
    this.cronExpression = StringUtils.isBlank(cronExpression) ? null : cronExpression.trim();
  }

  /**
   * @return whether commit-triggered maintenance is enabled
   */
  public boolean onCommit() {
    return onCommit;
  }

  /**
   * @return Quartz cron expression when set
   */
  @Nullable
  public String cronExpression() {
    return cronExpression;
  }

  /**
   * Validates this trigger for the given maintenance task kind.
   *
   * <p>Called from {@link TableMaintenancePolicies}, not from individual policy content {@code
   * validate()} methods.
   *
   * @param taskType maintenance task type
   */
  public void validate(TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    if (onCommit && !taskType.onCommitAllowed()) {
      throw new IllegalArgumentException(
          String.format(
              "schedule.onCommit=true is not allowed for maintenance task type %s",
              taskType.configKey()));
    }
    if (!onCommit && cronExpression == null) {
      throw new IllegalArgumentException(
          "schedule must set onCommit and/or cronExpression for automated maintenance");
    }
    if (cronExpression != null) {
      validateCronExpression(cronExpression);
    }
  }

  /**
   * Validates Quartz cron field count and token grammar.
   *
   * @param expression trimmed cron expression
   */
  public static void validateCronExpression(String expression) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(expression), "cronExpression must not be blank");
    String[] fields = expression.trim().split("\\s+");
    Preconditions.checkArgument(
        fields.length == 6 || fields.length == 7,
        "cronExpression must be a Quartz cron with 6 fields "
            + "(seconds minutes hours day-of-month month day-of-week) or 7 fields "
            + "(optional year), but got %s field(s): '%s'",
        fields.length,
        expression);
    for (int i = 0; i < fields.length; i++) {
      Preconditions.checkArgument(
          CRON_FIELD.matcher(fields[i]).matches(),
          "cronExpression field %s ('%s') contains illegal characters",
          i + 1,
          fields[i]);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MaintenanceTrigger)) {
      return false;
    }
    MaintenanceTrigger that = (MaintenanceTrigger) o;
    return onCommit == that.onCommit && Objects.equals(cronExpression, that.cronExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(onCommit, cronExpression);
  }

  @Override
  public String toString() {
    return "MaintenanceTrigger{onCommit=" + onCommit + ", cronExpression=" + cronExpression + '}';
  }
}
