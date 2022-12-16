/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.admin.backup;

import io.camunda.zeebe.gateway.impl.broker.request.BrokerRequest;
import io.camunda.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.camunda.zeebe.protocol.impl.encoding.BackupRequest;
import io.camunda.zeebe.protocol.impl.encoding.BackupStatusResponse;
import io.camunda.zeebe.protocol.management.BackupRequestType;
import io.camunda.zeebe.protocol.management.BackupStatusResponseDecoder;
import io.camunda.zeebe.transport.RequestType;
import io.camunda.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class BackupStatusRequest extends BrokerRequest<BackupStatusResponse> {

  protected final BackupRequest request = new BackupRequest();
  protected final BackupStatusResponse response = new BackupStatusResponse();

  public BackupStatusRequest() {
    super(BackupStatusResponseDecoder.SCHEMA_ID, BackupStatusResponseDecoder.TEMPLATE_ID);
    request.setType(BackupRequestType.QUERY_STATUS);
  }

  public long getBackupId() {
    return request.getBackupId();
  }

  public void setBackupId(final long backupId) {
    request.setBackupId(backupId);
  }

  @Override
  public int getPartitionId() {
    return request.getPartitionId();
  }

  @Override
  public void setPartitionId(final int partitionId) {
    request.setPartitionId(partitionId);
  }

  @Override
  public boolean addressesSpecificPartition() {
    return true;
  }

  @Override
  public boolean requiresPartitionId() {
    return true;
  }

  @Override
  public BufferWriter getRequestWriter() {
    return null;
  }

  @Override
  protected void setSerializedValue(final DirectBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void wrapResponse(final DirectBuffer buffer) {
    response.wrap(buffer, 0, buffer.capacity());
  }

  @Override
  protected BrokerResponse<BackupStatusResponse> readResponse() {
    return new BrokerResponse<>(response, response.getPartitionId(), -1);
  }

  @Override
  protected BackupStatusResponse toResponseDto(final DirectBuffer buffer) {
    return response;
  }

  @Override
  public String getType() {
    return "Backup#status";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.BACKUP;
  }

  @Override
  public int getLength() {
    return request.getLength();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    request.write(buffer, offset);
  }
}
