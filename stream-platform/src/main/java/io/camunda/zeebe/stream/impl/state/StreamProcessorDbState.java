/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.stream.impl.state;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;

public class StreamProcessorDbState {

  private final DbLastProcessedPositionState lastProcessedPositionState;

  public StreamProcessorDbState(
      final ZeebeDb zeebeDb, final TransactionContext transactionContext) {
    lastProcessedPositionState = new DbLastProcessedPositionState(zeebeDb, transactionContext);
  }

  public DbLastProcessedPositionState getLastProcessedPositionState() {
    return lastProcessedPositionState;
  }
}
