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
package org.apache.gravitino.idp.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupDTO {

  @Test
  public void testIdpGroupDTOBuilder() {
    IdpGroupDTO groupDTO =
        IdpGroupDTO.builder()
            .withGroupName("engineering")
            .withUsers(Arrays.asList("user1", "user2"))
            .build();

    Assertions.assertEquals("engineering", groupDTO.getGroupName());
    Assertions.assertEquals(Arrays.asList("user1", "user2"), groupDTO.getUsers());
  }

  @Test
  public void testEqualsAndHashCode() {
    IdpGroupDTO groupDTO1 =
        IdpGroupDTO.builder()
            .withGroupName("engineering")
            .withUsers(Arrays.asList("user1", "user2"))
            .build();

    IdpGroupDTO groupDTO2 =
        IdpGroupDTO.builder()
            .withGroupName("engineering")
            .withUsers(Arrays.asList("user1", "user2"))
            .build();

    Assertions.assertEquals(groupDTO1, groupDTO2);
    Assertions.assertEquals(groupDTO1.hashCode(), groupDTO2.hashCode());
  }

  @Test
  public void testIdpGroupDTOSerDe() throws JsonProcessingException {
    IdpGroupDTO groupDTO =
        IdpGroupDTO.builder()
            .withGroupName("test_group")
            .withUsers(Arrays.asList("user1", "user2"))
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(groupDTO);
    IdpGroupDTO deserialized = JsonUtils.objectMapper().readValue(json, IdpGroupDTO.class);

    Assertions.assertEquals(groupDTO, deserialized);
  }
}
