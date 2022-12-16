/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.streamprocessor;

import io.camunda.zeebe.engine.processing.streamprocessor.CommandProcessor.CommandControl;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectQueue;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.SideEffectProducer;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import java.util.function.Consumer;

/**
 * Decorates a command processor with simple accept and reject logic.
 *
 * <p>On accept it writes the state corresponding to successfully processing the command (e.g.
 * process instance creation: CREATE => CREATED); and responds if it was a client command that
 * should be responded to.
 *
 * <p>On reject it writes a command rejection
 *
 * @param <T> the record value type
 */
public final class CommandProcessorImpl<T extends UnifiedRecordValue>
    implements TypedRecordProcessor<T>, CommandControl<T> {

  private final SideEffectQueue sideEffectQueue = new SideEffectQueue();

  private final CommandProcessor<T> wrappedProcessor;

  private final KeyGenerator keyGenerator;
  private final StateWriter stateWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final TypedCommandWriter commandWriter;

  private boolean isAccepted;
  private long entityKey;

  private Intent newState;
  private T updatedValue;

  private RejectionType rejectionType;
  private String rejectionReason;
  private final TypedResponseWriter responseWriter;

  public CommandProcessorImpl(
      final CommandProcessor<T> commandProcessor,
      final KeyGenerator keyGenerator,
      final Writers writers) {
    wrappedProcessor = commandProcessor;
    this.keyGenerator = keyGenerator;
    stateWriter = writers.state();
    commandWriter = writers.command();
    rejectionWriter = writers.rejection();
    responseWriter = writers.response();
  }

  @Override
  public void processRecord(
      final TypedRecord<T> command, final Consumer<SideEffectProducer> sideEffect) {

    entityKey = command.getKey();

    sideEffect.accept(sideEffectQueue);
    sideEffectQueue.clear();

    final boolean shouldRespond = wrappedProcessor.onCommand(command, this, sideEffectQueue::add);

    final boolean respond = shouldRespond && command.hasRequestMetadata();

    if (isAccepted) {
      stateWriter.appendFollowUpEvent(entityKey, newState, updatedValue);
      wrappedProcessor.afterAccept(commandWriter, stateWriter, entityKey, newState, updatedValue);
      if (respond) {
        responseWriter.writeEventOnCommand(entityKey, newState, updatedValue, command);
      }
    } else {
      rejectionWriter.appendRejection(command, rejectionType, rejectionReason);
      if (respond) {
        responseWriter.writeRejectionOnCommand(command, rejectionType, rejectionReason);
      }
    }
  }

  @Override
  public ProcessingError tryHandleError(final TypedRecord<T> command, final Throwable error) {
    return wrappedProcessor.tryHandleError(command, error);
  }

  @Override
  public long accept(final Intent newState, final T updatedValue) {
    if (entityKey < 0) {
      entityKey = keyGenerator.nextKey();
    }

    isAccepted = true;
    this.newState = newState;
    this.updatedValue = updatedValue;
    return entityKey;
  }

  @Override
  public void reject(final RejectionType type, final String reason) {
    isAccepted = false;
    rejectionType = type;
    rejectionReason = reason;
  }
}
