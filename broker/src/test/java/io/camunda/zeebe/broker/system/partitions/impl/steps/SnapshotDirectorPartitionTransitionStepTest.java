/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system.partitions.impl.steps;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.atomix.raft.RaftServer.Role;
import io.atomix.raft.partition.RaftPartition;
import io.atomix.raft.partition.impl.RaftPartitionServer;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.partitions.TestPartitionTransitionContext;
import io.camunda.zeebe.broker.system.partitions.impl.AsyncSnapshotDirector;
import io.camunda.zeebe.broker.system.partitions.impl.steps.PartitionTransitionTestArgumentProviders.TransitionsThatShouldCloseService;
import io.camunda.zeebe.broker.system.partitions.impl.steps.PartitionTransitionTestArgumentProviders.TransitionsThatShouldDoNothing;
import io.camunda.zeebe.broker.system.partitions.impl.steps.PartitionTransitionTestArgumentProviders.TransitionsThatShouldInstallService;
import io.camunda.zeebe.scheduler.ActorSchedulingService;
import io.camunda.zeebe.scheduler.testing.TestActorFuture;
import io.camunda.zeebe.stream.impl.StreamProcessor;
import io.camunda.zeebe.util.health.HealthMonitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

class SnapshotDirectorPartitionTransitionStepTest {

  TestPartitionTransitionContext transitionContext = new TestPartitionTransitionContext();

  private SnapshotDirectorPartitionTransitionStep step;
  private final RaftPartition raftPartition = mock(RaftPartition.class);
  private final RaftPartitionServer raftServer = mock(RaftPartitionServer.class);
  private final ActorSchedulingService actorSchedulingService = mock(ActorSchedulingService.class);
  private final AsyncSnapshotDirector snapshotDirectorFromPrevRole =
      mock(AsyncSnapshotDirector.class);

  @BeforeEach
  void setup() {
    transitionContext.setComponentHealthMonitor(mock(HealthMonitor.class));
    transitionContext.setStreamProcessor(mock(StreamProcessor.class));
    transitionContext.setBrokerCfg(new BrokerCfg());

    when(raftPartition.getServer()).thenReturn(raftServer);
    transitionContext.setRaftPartition(raftPartition);

    when(actorSchedulingService.submitActor(any(), any()))
        .thenReturn(TestActorFuture.completedFuture(null));
    transitionContext.setActorSchedulingService(actorSchedulingService);

    when(snapshotDirectorFromPrevRole.closeAsync())
        .thenReturn(TestActorFuture.completedFuture(null));

    step = new SnapshotDirectorPartitionTransitionStep();
  }

  @ParameterizedTest
  @ArgumentsSource(TransitionsThatShouldCloseService.class)
  void shouldCloseExistingSnapshotDirector(final Role currentRole, final Role targetRole) {
    // given
    initializeContext(currentRole);

    // when
    step.prepareTransition(transitionContext, 1, targetRole).join();

    // then
    assertThat(transitionContext.getSnapshotDirector()).isNull();
    verify(snapshotDirectorFromPrevRole).closeAsync();
  }

  @ParameterizedTest
  @ArgumentsSource(TransitionsThatShouldInstallService.class)
  void shouldReInstallSnapshotDirector(final Role currentRole, final Role targetRole) {
    // given
    initializeContext(currentRole);
    final var existingSnapshotDirector = transitionContext.getSnapshotDirector();

    // when
    transitionTo(targetRole);

    // then
    assertThat(transitionContext.getSnapshotDirector())
        .isNotNull()
        .isNotEqualTo(existingSnapshotDirector);
  }

  @ParameterizedTest
  @ArgumentsSource(TransitionsThatShouldDoNothing.class)
  void shouldNotReInstallSnapshotDirector(final Role currentRole, final Role targetRole) {
    // given
    initializeContext(currentRole);
    final var existingSnapshotDirector = transitionContext.getSnapshotDirector();

    // when
    transitionTo(targetRole);

    // then
    assertThat(transitionContext.getSnapshotDirector()).isEqualTo(existingSnapshotDirector);
  }

  @ParameterizedTest
  @EnumSource(
      value = Role.class,
      names = {"FOLLOWER", "LEADER", "CANDIDATE"})
  void shouldCloseWhenTransitioningToInactive(final Role currentRole) {
    // given
    initializeContext(currentRole);

    // when
    transitionTo(Role.INACTIVE);

    // then
    assertThat(transitionContext.getSnapshotDirector()).isNull();
  }

  private void initializeContext(final Role currentRole) {
    transitionContext.setCurrentRole(currentRole);
    if (currentRole != null && currentRole != Role.INACTIVE) {
      transitionContext.setSnapshotDirector(snapshotDirectorFromPrevRole);
    }
  }

  private void transitionTo(final Role role) {
    step.prepareTransition(transitionContext, 1, role).join();
    step.transitionTo(transitionContext, 1, role).join();
    transitionContext.setCurrentRole(role);
  }
}
