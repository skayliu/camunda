/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.admin.backup;

import io.camunda.zeebe.gateway.api.util.StubbedBrokerClient;
import io.camunda.zeebe.gateway.api.util.StubbedBrokerClient.RequestStub;
import io.camunda.zeebe.gateway.impl.broker.response.BrokerError;
import io.camunda.zeebe.gateway.impl.broker.response.BrokerErrorResponse;
import io.camunda.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.camunda.zeebe.protocol.record.ErrorCode;
import java.util.HashMap;
import java.util.Map;

public class BackupStub
    implements RequestStub<BrokerBackupRequest, BrokerResponse<BackupResponse>> {

  private final Map<Integer, BrokerResponse<BackupResponse>> responses = new HashMap<>();

  @Override
  public void registerWith(final StubbedBrokerClient gateway) {
    gateway.registerHandler(BrokerBackupRequest.class, this);
  }

  @Override
  public BrokerResponse<BackupResponse> handle(final BrokerBackupRequest request) throws Exception {
    return responses.getOrDefault(
        request.getPartitionId(),
        new BrokerResponse<>(new BackupResponse(true, 1), request.getPartitionId(), -1));
  }

  public BackupStub withErrorResponseFor(final int partitionId) {
    responses.put(
        partitionId, new BrokerErrorResponse<>(new BrokerError(ErrorCode.INTERNAL_ERROR, "ERROR")));
    return this;
  }

  public BackupStub withResponse(final BackupResponse response, final int partitionId) {
    responses.put(partitionId, new BrokerResponse<>(response, partitionId, -1));
    return this;
  }
}
