/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.test.optimize;

import jakarta.ws.rs.core.Response;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.camunda.optimize.OptimizeRequestExecutor;
import org.camunda.optimize.dto.optimize.rest.FlowNodeIdsToNamesRequestDto;
import org.camunda.optimize.dto.optimize.rest.FlowNodeNamesResponseDto;

@AllArgsConstructor
public class FlowNodeNamesClient {
  private final Supplier<OptimizeRequestExecutor> requestExecutorSupplier;

  public FlowNodeNamesResponseDto getFlowNodeNames(FlowNodeIdsToNamesRequestDto requestDto) {
    return getRequestExecutor()
        .buildGetFlowNodeNames(requestDto)
        .execute(FlowNodeNamesResponseDto.class, Response.Status.OK.getStatusCode());
  }

  private OptimizeRequestExecutor getRequestExecutor() {
    return requestExecutorSupplier.get();
  }
}
