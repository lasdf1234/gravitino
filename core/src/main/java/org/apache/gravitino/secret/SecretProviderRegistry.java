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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.MapUtils;

/** Registry of configured {@link GravitinoSecretProvider} instances. */
public class SecretProviderRegistry {

  /** Configuration prefix for secret providers. */
  public static final String PREFIX = "gravitino.secret.";

  private static final String PROVIDERS_KEY = "providers";
  private static final String PROVIDER_PREFIX = "provider.";
  private static final String CLASS_NAME_KEY = "className";
  private static final Splitter PROVIDER_NAME_SPLITTER =
      Splitter.on(',').trimResults().omitEmptyStrings();

  private Map<String, GravitinoSecretProvider> providers = ImmutableMap.of();

  /**
   * Initializes the registry from server configuration.
   *
   * @param config the full Gravitino configuration map
   */
  public void init(Map<String, String> config) {
    Map<String, String> secretConfig = MapUtils.getPrefixMap(config, PREFIX);
    String providerNames = secretConfig.getOrDefault(PROVIDERS_KEY, "");
    if (providerNames.isBlank()) {
      providers = ImmutableMap.of();
      return;
    }

    ImmutableMap.Builder<String, GravitinoSecretProvider> builder = ImmutableMap.builder();
    for (String providerName : PROVIDER_NAME_SPLITTER.splitToList(providerNames)) {
      Map<String, String> providerConfig =
          MapUtils.getPrefixMap(secretConfig, PROVIDER_PREFIX + providerName + ".");
      String className = providerConfig.get(CLASS_NAME_KEY);
      if (className == null || className.isBlank()) {
        throw new GravitinoRuntimeException(
            "Missing className for secret provider: %s", providerName);
      }
      builder.put(providerName, loadProvider(className, providerName));
    }
    providers = builder.build();
  }

  /**
   * Returns the provider registered under the given name.
   *
   * @param name the configured provider name
   * @return the provider, or {@code null} if not registered
   */
  @Nullable
  public GravitinoSecretProvider get(String name) {
    return providers.get(name);
  }

  /**
   * Returns the configured provider names.
   *
   * @return the provider names
   */
  public List<String> listNames() {
    return ImmutableList.copyOf(providers.keySet());
  }

  /**
   * Returns metadata for all configured providers.
   *
   * @return provider metadata
   */
  public List<SecretProviderInfo> listProviderInfos() {
    ImmutableList.Builder<SecretProviderInfo> builder = ImmutableList.builder();
    for (Map.Entry<String, GravitinoSecretProvider> entry : providers.entrySet()) {
      builder.add(new SecretProviderInfo(entry.getKey(), entry.getValue().type()));
    }
    return builder.build();
  }

  private GravitinoSecretProvider loadProvider(String className, String providerName) {
    try {
      Class<?> providerClass = Class.forName(className);
      Object instance = providerClass.getDeclaredConstructor().newInstance();
      if (!(instance instanceof GravitinoSecretProvider)) {
        throw new GravitinoRuntimeException(
            "Secret provider class %s does not implement GravitinoSecretProvider", className);
      }
      return (GravitinoSecretProvider) instance;
    } catch (ReflectiveOperationException e) {
      throw new GravitinoRuntimeException(
          e, "Failed to load secret provider %s with class %s", providerName, className);
    }
  }

  /** Returns an empty registry for tests. */
  static SecretProviderRegistry empty() {
    SecretProviderRegistry registry = new SecretProviderRegistry();
    registry.providers = Collections.emptyMap();
    return registry;
  }
}
