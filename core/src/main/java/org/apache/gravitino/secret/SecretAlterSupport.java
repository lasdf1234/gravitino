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

package org.apache.gravitino.secret;

import static org.apache.gravitino.secret.SecretConstants.SECRET_KEYS_PROPERTY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.file.FilesetChange;

/** Bridges secret alter operations to property changes catalog backends understand. */
public final class SecretAlterSupport {

  private SecretAlterSupport() {}

  /**
   * Returns whether the catalog changes include secret alter operations.
   *
   * @param changes the catalog changes
   * @return true if secret changes are present
   */
  public static boolean containsSecretChanges(CatalogChange... changes) {
    return Arrays.stream(changes).anyMatch(SecretAlterSupport::isSecretCatalogChange);
  }

  /**
   * Returns whether the schema changes include secret alter operations.
   *
   * @param changes the schema changes
   * @return true if secret changes are present
   */
  public static boolean containsSecretChanges(SchemaChange... changes) {
    return Arrays.stream(changes).anyMatch(SecretAlterSupport::isSecretSchemaChange);
  }

  /**
   * Returns whether the fileset changes include secret alter operations.
   *
   * @param changes the fileset changes
   * @return true if secret changes are present
   */
  public static boolean containsSecretChanges(FilesetChange... changes) {
    return Arrays.stream(changes).anyMatch(SecretAlterSupport::isSecretFilesetChange);
  }

  /**
   * Returns whether schema changes require secret-aware preparation.
   *
   * @param currentProps the current entity properties
   * @param changes the requested schema changes
   * @return true if secret preparation is required
   */
  public static boolean requiresSecretAlterPreparation(
      Map<String, String> currentProps, SchemaChange... changes) {
    if (containsSecretChanges(changes)) {
      return true;
    }
    Map<String, String> props = currentProps == null ? Map.of() : currentProps;
    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.RemoveProperty) {
        String property = ((SchemaChange.RemoveProperty) change).getProperty();
        if (SECRET_KEYS_PROPERTY.equals(property)
            || SecretPropertyHelper.secretKeys(props).contains(property)) {
          return true;
        }
      } else if (change instanceof SchemaChange.SetProperty) {
        if (SecretPropertyHelper.secretKeys(props)
            .contains(((SchemaChange.SetProperty) change).getProperty())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns whether fileset changes require secret-aware preparation.
   *
   * @param currentProps the current entity properties
   * @param changes the requested fileset changes
   * @return true if secret preparation is required
   */
  public static boolean requiresSecretAlterPreparation(
      Map<String, String> currentProps, FilesetChange... changes) {
    if (containsSecretChanges(changes)) {
      return true;
    }
    Map<String, String> props = currentProps == null ? Map.of() : currentProps;
    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.RemoveProperty) {
        String property = ((FilesetChange.RemoveProperty) change).getProperty();
        if (SECRET_KEYS_PROPERTY.equals(property)
            || SecretPropertyHelper.secretKeys(props).contains(property)) {
          return true;
        }
      } else if (change instanceof FilesetChange.SetProperty) {
        if (SecretPropertyHelper.secretKeys(props)
            .contains(((FilesetChange.SetProperty) change).getProperty())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Rewrites schema secret alter operations into property changes.
   *
   * @param currentProps the current schema properties
   * @param entityId the schema entity identifier
   * @param changes the requested schema changes
   * @param registry the secret provider registry
   * @return the prepared schema changes
   */
  public static SchemaChange[] prepareSchemaChanges(
      Map<String, String> currentProps,
      long entityId,
      SchemaChange[] changes,
      SecretProviderRegistry registry) {
    if (!requiresSecretAlterPreparation(currentProps, changes)) {
      return changes;
    }
    requireRegistry(registry);

    Map<String, String> working = copyProps(currentProps);
    String initialSecretKeys = working.get(SECRET_KEYS_PROPERTY);
    List<SchemaChange> result = new ArrayList<>();

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetSecretBinding) {
        SchemaChange.SetSecretBinding binding = (SchemaChange.SetSecretBinding) change;
        SecretPropertyHelper.applySetSecretBinding(
            working,
            "schema",
            entityId,
            binding.getProperty(),
            binding.getProvider(),
            binding.getValue(),
            registry);
        result.add(
            SchemaChange.setProperty(binding.getProperty(), working.get(binding.getProperty())));
      } else if (change instanceof SchemaChange.SetSecretReference) {
        SchemaChange.SetSecretReference reference = (SchemaChange.SetSecretReference) change;
        SecretPropertyHelper.applySetSecretReference(
            working,
            "schema",
            entityId,
            reference.getProperty(),
            reference.getProvider(),
            reference.getMount(),
            reference.getPath(),
            registry);
      } else if (change instanceof SchemaChange.RemoveProperty) {
        SchemaChange.RemoveProperty remove = (SchemaChange.RemoveProperty) change;
        SecretPropertyHelper.applyRemoveProperty(
            working, "schema", entityId, remove.getProperty(), registry);
        result.add(change);
      } else if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty set = (SchemaChange.SetProperty) change;
        SecretPropertyHelper.validatePlainSetProperty(working, set.getProperty(), set.getValue());
        working.put(set.getProperty(), set.getValue());
        result.add(change);
      } else {
        result.add(change);
      }
    }

    appendSecretKeysChange(
        result,
        initialSecretKeys,
        working.get(SECRET_KEYS_PROPERTY),
        SchemaChange::setProperty,
        SchemaChange::removeProperty);
    return result.toArray(new SchemaChange[0]);
  }

  /**
   * Rewrites fileset secret alter operations into property changes.
   *
   * @param currentProps the current fileset properties
   * @param entityId the fileset entity identifier
   * @param changes the requested fileset changes
   * @param registry the secret provider registry
   * @return the prepared fileset changes
   */
  public static FilesetChange[] prepareFilesetChanges(
      Map<String, String> currentProps,
      long entityId,
      FilesetChange[] changes,
      SecretProviderRegistry registry) {
    if (!requiresSecretAlterPreparation(currentProps, changes)) {
      return changes;
    }
    requireRegistry(registry);

    Map<String, String> working = copyProps(currentProps);
    String initialSecretKeys = working.get(SECRET_KEYS_PROPERTY);
    List<FilesetChange> result = new ArrayList<>();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetSecretBinding) {
        FilesetChange.SetSecretBinding binding = (FilesetChange.SetSecretBinding) change;
        SecretPropertyHelper.applySetSecretBinding(
            working,
            "fileset",
            entityId,
            binding.getProperty(),
            binding.getProvider(),
            binding.getValue(),
            registry);
        result.add(
            FilesetChange.setProperty(binding.getProperty(), working.get(binding.getProperty())));
      } else if (change instanceof FilesetChange.SetSecretReference) {
        FilesetChange.SetSecretReference reference = (FilesetChange.SetSecretReference) change;
        SecretPropertyHelper.applySetSecretReference(
            working,
            "fileset",
            entityId,
            reference.getProperty(),
            reference.getProvider(),
            reference.getMount(),
            reference.getPath(),
            registry);
      } else if (change instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty remove = (FilesetChange.RemoveProperty) change;
        SecretPropertyHelper.applyRemoveProperty(
            working, "fileset", entityId, remove.getProperty(), registry);
        result.add(change);
      } else if (change instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty set = (FilesetChange.SetProperty) change;
        SecretPropertyHelper.validatePlainSetProperty(working, set.getProperty(), set.getValue());
        working.put(set.getProperty(), set.getValue());
        result.add(change);
      } else {
        result.add(change);
      }
    }

    appendSecretKeysChange(
        result,
        initialSecretKeys,
        working.get(SECRET_KEYS_PROPERTY),
        FilesetChange::setProperty,
        FilesetChange::removeProperty);
    return result.toArray(new FilesetChange[0]);
  }

  /**
   * Applies a catalog change to catalog properties with secret-aware handling.
   *
   * @param props the mutable catalog properties
   * @param entityId the catalog entity identifier
   * @param change the catalog change
   * @param registry the secret provider registry
   */
  public static void applyCatalogChangeToProperties(
      Map<String, String> props,
      long entityId,
      CatalogChange change,
      @Nullable SecretProviderRegistry registry) {
    if (change instanceof CatalogChange.SetSecretBinding) {
      requireRegistry(registry);
      CatalogChange.SetSecretBinding binding = (CatalogChange.SetSecretBinding) change;
      SecretPropertyHelper.applySetSecretBinding(
          props,
          "catalog",
          entityId,
          binding.getProperty(),
          binding.getProvider(),
          binding.getValue(),
          registry);
    } else if (change instanceof CatalogChange.SetSecretReference) {
      requireRegistry(registry);
      CatalogChange.SetSecretReference reference = (CatalogChange.SetSecretReference) change;
      SecretPropertyHelper.applySetSecretReference(
          props,
          "catalog",
          entityId,
          reference.getProperty(),
          reference.getProvider(),
          reference.getMount(),
          reference.getPath(),
          registry);
    } else if (change instanceof CatalogChange.RemoveProperty) {
      if (registry != null) {
        CatalogChange.RemoveProperty remove = (CatalogChange.RemoveProperty) change;
        SecretPropertyHelper.applyRemoveProperty(
            props, "catalog", entityId, remove.getProperty(), registry);
      } else {
        CatalogChange.RemoveProperty remove = (CatalogChange.RemoveProperty) change;
        if (SECRET_KEYS_PROPERTY.equals(remove.getProperty())) {
          throw new IllegalArgumentException("Client must not manage gravitino.secret.keys");
        }
        props.remove(remove.getProperty());
      }
    } else if (change instanceof CatalogChange.SetProperty) {
      CatalogChange.SetProperty set = (CatalogChange.SetProperty) change;
      SecretPropertyHelper.validatePlainSetProperty(props, set.getProperty(), set.getValue());
      props.put(set.getProperty(), set.getValue());
    }
  }

  private static Map<String, String> copyProps(Map<String, String> currentProps) {
    return currentProps == null ? new HashMap<>() : new HashMap<>(currentProps);
  }

  private static void requireRegistry(SecretProviderRegistry registry) {
    if (registry == null) {
      throw new IllegalStateException(
          "Secret bindings/references were provided but secret provider registry is not"
              + " initialized");
    }
  }

  private static <T> void appendSecretKeysChange(
      List<T> result,
      String initialSecretKeys,
      String currentSecretKeys,
      java.util.function.BiFunction<String, String, T> setProperty,
      java.util.function.Function<String, T> removeProperty) {
    if (Objects.equals(initialSecretKeys, currentSecretKeys)) {
      return;
    }
    if (currentSecretKeys == null || currentSecretKeys.isBlank()) {
      result.add(removeProperty.apply(SECRET_KEYS_PROPERTY));
    } else {
      result.add(setProperty.apply(SECRET_KEYS_PROPERTY, currentSecretKeys));
    }
  }

  private static boolean isSecretCatalogChange(CatalogChange change) {
    return change instanceof CatalogChange.SetSecretBinding
        || change instanceof CatalogChange.SetSecretReference;
  }

  private static boolean isSecretSchemaChange(SchemaChange change) {
    return change instanceof SchemaChange.SetSecretBinding
        || change instanceof SchemaChange.SetSecretReference;
  }

  private static boolean isSecretFilesetChange(FilesetChange change) {
    return change instanceof FilesetChange.SetSecretBinding
        || change instanceof FilesetChange.SetSecretReference;
  }
}
