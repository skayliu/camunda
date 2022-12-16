/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system.partitions.impl.steps;

import io.atomix.raft.RaftServer.Role;
import io.camunda.zeebe.broker.system.partitions.PartitionTransitionContext;
import io.camunda.zeebe.broker.system.partitions.PartitionTransitionStep;
import io.camunda.zeebe.broker.transport.partitionapi.InterPartitionCommandReceiverActor;
import io.camunda.zeebe.broker.transport.partitionapi.InterPartitionCommandSenderService;
import io.camunda.zeebe.scheduler.future.ActorFuture;

public final class InterPartitionCommandServiceStep implements PartitionTransitionStep {

  @Override
  public ActorFuture<Void> prepareTransition(
      final PartitionTransitionContext context, final long term, final Role targetRole) {
    final ActorFuture<Void> closeFuture = context.getConcurrencyControl().createFuture();
    final var receiverClosed = closeReceiver(context);
    final var senderClosed = closeSender(context);
    receiverClosed.onComplete(
        (ignore, error) -> {
          if (error != null) {
            closeFuture.completeExceptionally(
                "Failed to close InterPartitionCommandReceiver", error);
          } else {
            waitForSenderToClose(closeFuture, senderClosed);
          }
        });
    return closeFuture;
  }

  @Override
  public ActorFuture<Void> transitionTo(
      final PartitionTransitionContext context, final long term, final Role targetRole) {
    if (targetRole == Role.LEADER && context.getPartitionCommandReceiver() == null) {
      final ActorFuture<Void> installFuture = context.getConcurrencyControl().createFuture();
      final var receiverInstalled = installReceiver(context);
      final var senderInstalled = installSender(context);
      receiverInstalled.onComplete(
          (ignore, error) -> {
            if (error != null) {
              installFuture.completeExceptionally(
                  "Failed to install InterPartitionCommandReceiver", error);
            } else {
              waitForSenderToStart(installFuture, senderInstalled);
            }
          });
      return installFuture;
    }
    return context.getConcurrencyControl().createCompletedFuture();
  }

  @Override
  public String getName() {
    return "InterPartitionCommandService";
  }

  private void completeAfterFuture(
      final ActorFuture<Void> futureToComplete,
      final ActorFuture<Void> futureToWait,
      final String errorMessage) {
    futureToWait.onComplete(
        (ok, senderError) -> {
          if (senderError != null) {
            futureToComplete.completeExceptionally(errorMessage, senderError);
          } else {
            futureToComplete.complete(null);
          }
        });
  }

  private void waitForSenderToClose(
      final ActorFuture<Void> closeFuture, final ActorFuture<Void> senderClosed) {
    completeAfterFuture(closeFuture, senderClosed, "Failed to close InterPartitionCommandSender");
  }

  private void waitForSenderToStart(
      final ActorFuture<Void> startFuture, final ActorFuture<Void> senderInstalled) {
    completeAfterFuture(
        startFuture, senderInstalled, "Failed to install InterPartitionCommandSender");
  }

  private ActorFuture<Void> installReceiver(final PartitionTransitionContext context) {
    final ActorFuture<Void> future = context.getConcurrencyControl().createFuture();

    context
        .getLogStream()
        .newLogStreamWriter()
        .onComplete(
            (writer, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
                return;
              }
              final var receiver =
                  new InterPartitionCommandReceiverActor(
                      context.getNodeId(),
                      context.getPartitionId(),
                      context.getClusterCommunicationService(),
                      writer);
              context.getActorSchedulingService().submitActor(receiver);
              context.setPartitionCommandReceiver(receiver);
              context.getCheckpointProcessor().addCheckpointListener(receiver);
              future.complete(null);
            });
    return future;
  }

  private ActorFuture<Void> installSender(final PartitionTransitionContext context) {
    final ActorFuture<Void> future = context.getConcurrencyControl().createFuture();

    final var sender =
        new InterPartitionCommandSenderService(
            context.getClusterCommunicationService(), context.getPartitionId());
    final var actorStarted = context.getActorSchedulingService().submitActor(sender);
    actorStarted.onComplete(
        (ignore, error) -> {
          if (error == null) {
            context.setPartitionCommandSender(sender);
            context.getTopologyManager().addTopologyPartitionListener(sender);
            context.getCheckpointProcessor().addCheckpointListener(sender);
            future.complete(null);
          } else {
            future.completeExceptionally(error);
          }
        });

    return future;
  }

  private ActorFuture<Void> closeReceiver(final PartitionTransitionContext context) {
    final var receiver = context.getPartitionCommandReceiver();
    if (receiver != null) {
      final ActorFuture<Void> receiverCloseFuture = receiver.closeAsync();
      receiverCloseFuture.onComplete((res, error) -> context.setPartitionCommandReceiver(null));
      return receiverCloseFuture;
    } else {
      return context.getConcurrencyControl().createCompletedFuture();
    }
  }

  private ActorFuture<Void> closeSender(final PartitionTransitionContext context) {
    final var sender = context.getPartitionCommandSender();
    if (sender != null) {
      context.getTopologyManager().removeTopologyPartitionListener(sender);
      final ActorFuture<Void> receiverCloseFuture = sender.closeAsync();
      receiverCloseFuture.onComplete((res, error) -> context.setPartitionCommandReceiver(null));
      return receiverCloseFuture;
    } else {
      return context.getConcurrencyControl().createCompletedFuture();
    }
  }
}
