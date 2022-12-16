/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.stream.api;

import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.records.ExceededBatchRecordSizeException;
import io.camunda.zeebe.util.Either;

/** Builder to compose the processing result */
public interface ProcessingResultBuilder {

  /**
   * Appends a record to the result
   *
   * @return returns itself for method chaining
   * @throws ExceededBatchRecordSizeException if the appended record doesn't fit into the
   *     RecordBatch
   */
  default ProcessingResultBuilder appendRecord(
      final long key,
      final RecordType type,
      final Intent intent,
      final RejectionType rejectionType,
      final String rejectionReason,
      final RecordValue value)
      throws RuntimeException {
    final var either =
        appendRecordReturnEither(key, type, intent, rejectionType, rejectionReason, value);

    if (either.isLeft()) {
      // This is how we handled too big record batches as well, except that this is now a
      // different place. Before an exception was raised during the writing, now it is during
      // processing. Both will lead to the onError call, such that the RecordProcessors can handle
      // this case.
      throw either.getLeft();
    }
    return either.get();
  }

  /**
   * Appends a record to the result, returns an {@link Either<RuntimeException,
   * ProcessingResultBuilder>} which indicates whether the appending was successful or not. This is
   * useful in case were potentially we could reach the record batch limit size. The return either
   * allows to handle such error case gracefully.
   *
   * @return returns either a failure or itself for chaining
   */
  Either<RuntimeException, ProcessingResultBuilder> appendRecordReturnEither(
      final long key,
      final RecordType type,
      final Intent intent,
      final RejectionType rejectionType,
      final String rejectionReason,
      final RecordValue value);

  /**
   * Sets the response for the result; will be overwritten if called more than once
   *
   * @return returns itself for method chaining
   */
  ProcessingResultBuilder withResponse(
      final RecordType type,
      final long key,
      final Intent intent,
      final UnpackedObject value,
      final ValueType valueType,
      final RejectionType rejectionType,
      final String rejectionReason,
      final long requestId,
      final int requestStreamId);

  /**
   * Appends a task to be executed after a successful commit ProcessingResultBuilder (replacement
   * for side effects)
   *
   * @return returns itself for method chaining
   */
  ProcessingResultBuilder appendPostCommitTask(PostCommitTask task);

  /**
   * Resets itself with the post commit tasks reset
   *
   * @return itself for method chaining
   */
  ProcessingResultBuilder resetPostCommitTasks();

  ProcessingResult build();

  boolean canWriteEventOfLength(int eventLength);
}
