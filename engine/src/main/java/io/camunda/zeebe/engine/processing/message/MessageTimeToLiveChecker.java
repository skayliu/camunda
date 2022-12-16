/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.message;

import io.camunda.zeebe.engine.state.immutable.MessageState;
import io.camunda.zeebe.engine.state.message.StoredMessage;
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.camunda.zeebe.protocol.record.intent.MessageIntent;
import io.camunda.zeebe.scheduler.clock.ActorClock;
import io.camunda.zeebe.stream.api.scheduling.Task;
import io.camunda.zeebe.stream.api.scheduling.TaskResult;
import io.camunda.zeebe.stream.api.scheduling.TaskResultBuilder;

public final class MessageTimeToLiveChecker implements Task {

  private final MessageState messageState;

  private final MessageRecord deleteMessageCommand = new MessageRecord();

  public MessageTimeToLiveChecker(final MessageState messageState) {
    this.messageState = messageState;
  }

  @Override
  public TaskResult execute(final TaskResultBuilder taskResultBuilder) {
    messageState.visitMessagesWithDeadlineBefore(
        ActorClock.currentTimeMillis(),
        message -> writeDeleteMessageCommand(message, taskResultBuilder));
    return taskResultBuilder.build();
  }

  private boolean writeDeleteMessageCommand(
      final StoredMessage storedMessage, final TaskResultBuilder taskResultBuilder) {
    final var message = storedMessage.getMessage();

    deleteMessageCommand.reset();
    deleteMessageCommand
        .setName(message.getName())
        .setCorrelationKey(message.getCorrelationKey())
        .setTimeToLive(message.getTimeToLive())
        .setVariables(message.getVariablesBuffer());

    if (message.hasMessageId()) {
      deleteMessageCommand.setMessageId(message.getMessageIdBuffer());
    }

    return taskResultBuilder.appendCommandRecord(
        storedMessage.getMessageKey(), MessageIntent.EXPIRE, deleteMessageCommand);
  }
}
