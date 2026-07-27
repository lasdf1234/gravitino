/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache Spark's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/NamespaceChange.java

package org.apache.gravitino;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** NamespaceChange class to set the property and value pairs for the namespace. */
@Evolving
public interface SchemaChange {

  /**
   * SchemaChange class to set the property and value pairs for the schema.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The SchemaChange object.
   */
  static SchemaChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * SchemaChange class to remove a property from the schema.
   *
   * @param property The property name to remove.
   * @return The SchemaChange object.
   */
  static SchemaChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Creates a schema change to set a write-through secret binding.
   *
   * @param property The secret property name.
   * @param provider The secret provider name.
   * @param value The plaintext secret value.
   * @return The schema change.
   */
  static SchemaChange setSecretBinding(String property, String provider, String value) {
    return new SetSecretBinding(property, provider, value);
  }

  /**
   * Creates a schema change to set an external secret reference.
   *
   * @param property The secret property name.
   * @param provider The secret provider name.
   * @param mount The optional mount locator segment.
   * @param path The optional path locator segment.
   * @return The schema change.
   */
  static SchemaChange setSecretReference(
      String property, String provider, @Nullable String mount, @Nullable String path) {
    return new SetSecretReference(property, provider, mount, path);
  }

  /** SchemaChange class to set the property and value pairs for the schema. */
  final class SetProperty implements SchemaChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property to be set.
     *
     * @return The name of the property.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value of the property to be set.
     *
     * @return The value of the property.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they have the same property and value.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property setting; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return Objects.equals(property, that.property) && Objects.equals(value, that.value);
    }

    /**
     * Generates a hash code for this SetProperty instance. The hash code is based on both the
     * property name and its value.
     *
     * @return A hash code value for this property setting.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Provides a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property name and its value.
     *
     * @return A string summary of the property setting.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** SchemaChange class to remove a property from the schema. */
  final class RemoveProperty implements SchemaChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed.
     *
     * @return The name of the property for removal.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property removal; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return Objects.equals(property, that.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance. This hash code is based on the
     * property name that is to be removed.
     *
     * @return A hash code value for this property removal operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    /**
     * Provides a string representation of the RemoveProperty instance. This string format includes
     * the class name followed by the property name to be removed.
     *
     * @return A string summary of the property removal operation.
     */
    @Override
    public String toString() {
      return "REMOVEPROPERTY " + property;
    }
  }

  /** SchemaChange class to set a write-through secret binding. */
  final class SetSecretBinding implements SchemaChange {
    private final String property;
    private final String provider;
    private final String value;

    private SetSecretBinding(String property, String provider, String value) {
      this.property = property;
      this.provider = provider;
      this.value = value;
    }

    /**
     * Returns the secret property name.
     *
     * @return the property name
     */
    public String getProperty() {
      return property;
    }

    /**
     * Returns the secret provider name.
     *
     * @return the provider name
     */
    public String getProvider() {
      return provider;
    }

    /**
     * Returns the plaintext secret value.
     *
     * @return the plaintext value
     */
    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetSecretBinding that = (SetSecretBinding) o;
      return Objects.equals(property, that.property)
          && Objects.equals(provider, that.provider)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, provider, value);
    }

    @Override
    public String toString() {
      return "SETSECRETBINDING " + property + " " + provider + " " + value;
    }
  }

  /** SchemaChange class to set an external secret reference. */
  final class SetSecretReference implements SchemaChange {
    private final String property;
    private final String provider;
    private final String mount;
    private final String path;

    private SetSecretReference(
        String property, String provider, @Nullable String mount, @Nullable String path) {
      this.property = property;
      this.provider = provider;
      this.mount = mount;
      this.path = path;
    }

    /**
     * Returns the secret property name.
     *
     * @return the property name
     */
    public String getProperty() {
      return property;
    }

    /**
     * Returns the secret provider name.
     *
     * @return the provider name
     */
    public String getProvider() {
      return provider;
    }

    /**
     * Returns the optional mount locator segment.
     *
     * @return the mount segment, or null if not set
     */
    @Nullable
    public String getMount() {
      return mount;
    }

    /**
     * Returns the optional path locator segment.
     *
     * @return the path segment, or null if not set
     */
    @Nullable
    public String getPath() {
      return path;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetSecretReference that = (SetSecretReference) o;
      return Objects.equals(property, that.property)
          && Objects.equals(provider, that.provider)
          && Objects.equals(mount, that.mount)
          && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, provider, mount, path);
    }

    @Override
    public String toString() {
      return "SETSECRETREFERENCE " + property + " " + provider + " " + mount + " " + path;
    }
  }
}
