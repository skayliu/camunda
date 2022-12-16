/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.admin.backup;

/**
 * Response of take backup request, received from the broker.
 *
 * @param created true when backup is created, false when it is rejected because the checkpoint
 *     already exists
 * @param checkpointId if created = false then the latest checkpointId in the partition, otherwise
 *     same as the backupId in the request.
 */
public record BackupResponse(boolean created, long checkpointId) {}
