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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update a schema. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = SchemaUpdateRequest.SetSchemaPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = SchemaUpdateRequest.RemoveSchemaPropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(
      value = SchemaUpdateRequest.SetSchemaSecretBindingRequest.class,
      name = "setSecretBinding"),
  @JsonSubTypes.Type(
      value = SchemaUpdateRequest.SetSchemaSecretReferenceRequest.class,
      name = "setSecretReference")
})
public interface SchemaUpdateRequest extends RESTRequest {

  /**
   * The schema change that is requested.
   *
   * @return An instance of SchemaChange.
   */
  SchemaChange schemaChange();

  /** Represents a request to set a property of a schema. */
  @EqualsAndHashCode
  @ToString
  class SetSchemaPropertyRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Creates a new SetSchemaPropertyRequest.
     *
     * @param property The property to set.
     * @param value The value to set.
     */
    public SetSchemaPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** Default constructor for Jackson deserialization. */
    public SetSchemaPropertyRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(value), "\"value\" field is required and cannot be empty");
    }

    /**
     * Returns the schema change.
     *
     * @return An instance of SchemaChange.
     */
    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.setProperty(property, value);
    }
  }

  /** Represents a request to remove a property of a schema. */
  @EqualsAndHashCode
  @ToString
  class RemoveSchemaPropertyRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Creates a new RemoveSchemaPropertyRequest.
     *
     * @param property The property to remove.
     */
    public RemoveSchemaPropertyRequest(String property) {
      this.property = property;
    }

    /** Default constructor for Jackson deserialization. */
    public RemoveSchemaPropertyRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }

    /**
     * Returns the schema change.
     *
     * @return An instance of SchemaChange.
     */
    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.removeProperty(property);
    }
  }

  /** Represents a request to set a write-through secret binding on a schema. */
  @EqualsAndHashCode
  @ToString
  class SetSchemaSecretBindingRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("provider")
    private final String provider;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Creates a new SetSchemaSecretBindingRequest.
     *
     * @param property The secret property key.
     * @param provider The secret provider name.
     * @param value The plaintext secret value.
     */
    public SetSchemaSecretBindingRequest(String property, String provider, String value) {
      this.property = property;
      this.provider = provider;
      this.value = value;
    }

    /** Default constructor for Jackson deserialization. */
    public SetSchemaSecretBindingRequest() {
      this(null, null, null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(provider), "\"provider\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(value), "\"value\" field is required and cannot be empty");
    }

    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.setSecretBinding(property, provider, value);
    }
  }

  /** Represents a request to set an external secret reference on a schema. */
  @EqualsAndHashCode
  @ToString
  class SetSchemaSecretReferenceRequest implements SchemaUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("provider")
    private final String provider;

    @Getter
    @JsonProperty("mount")
    private final String mount;

    @Getter
    @JsonProperty("path")
    private final String path;

    /**
     * Creates a new SetSchemaSecretReferenceRequest.
     *
     * @param property The secret property key.
     * @param provider The secret provider name.
     * @param mount The optional mount locator segment.
     * @param path The optional path locator segment.
     */
    public SetSchemaSecretReferenceRequest(
        String property, String provider, String mount, String path) {
      this.property = property;
      this.provider = provider;
      this.mount = mount;
      this.path = path;
    }

    /** Default constructor for Jackson deserialization. */
    public SetSchemaSecretReferenceRequest() {
      this(null, null, null, null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(provider), "\"provider\" field is required and cannot be empty");
    }

    @Override
    public SchemaChange schemaChange() {
      return SchemaChange.setSecretReference(property, provider, mount, path);
    }
  }
}
