/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.ordering;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

import io.camunda.zeebe.scheduler.ActorCondition;
import io.camunda.zeebe.scheduler.future.CompletableActorFuture;
import io.camunda.zeebe.scheduler.testing.ControlledActorSchedulerRule;
import java.time.Duration;
import org.junit.Rule;
import org.junit.Test;

public final class RunnableOrderingTests {
  private static final String ONE = "one";
  private static final String TWO = "two";
  private static final String THREE = "three";
  private static final String FOUR = "four";
  private static final String FIVE = "five";

  @Rule
  public final ControlledActorSchedulerRule schedulerRule = new ControlledActorSchedulerRule();

  @Test
  public void shouldRunAllActionsInAnyOrder() {
    // given
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(runnable(ONE));
            actor.run(runnable(TWO));
            actor.run(runnable(THREE));
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // then
    // all actions are performed in any order
    assertThat(actor.actions).containsOnly(ONE, TWO, THREE);
  }

  @Test
  public void shouldFinishCurrentRunnableAfterExecutingNext() {
    // given
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  actor.run(runnable(TWO)); // this is executed after the current runnable returns
                  actions.add(ONE);
                });

            actor.run(runnable(THREE));
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // then
    assertThat(actor.actions).containsSequence(newArrayList(ONE, TWO));
    assertThat(actor.actions).containsOnly(ONE, TWO, THREE);
  }

  @Test
  public void submitTest() {
    // given
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  actor.submit(runnable(TWO));
                  actions.add(ONE);
                });

            actor.run(runnable(THREE));
            actor.submit(runnable(FOUR));
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // then
    // no guarantee of ordering between (ONE, THREE) and (TWO, FOUR), but the following constraints
    // must hold:
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, TWO));
    assertThat(actor.actions).containsSubsequence(newArrayList(THREE, TWO));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, FOUR));
    assertThat(actor.actions).containsSubsequence(newArrayList(THREE, FOUR));
  }

  @Test
  public void runOnCompletionFutureTest() {
    // given
    final CompletableActorFuture<Void> future = CompletableActorFuture.completed(null);
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  actor.runOnCompletion(future, futureConsumer(TWO));
                  actions.add(ONE);
                });

            actor.run(runnable(THREE));
            actor.runOnCompletion(future, futureConsumer(FOUR));
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // then
    // no guarantee of ordering between (ONE, THREE) and (TWO, FOUR), but the following constraints
    // must hold:
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, TWO));
    assertThat(actor.actions).containsSubsequence(newArrayList(THREE, TWO));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, FOUR));
    assertThat(actor.actions).containsSubsequence(newArrayList(THREE, FOUR));
  }

  @Test
  public void blockPhaseUntilCompletionFutureTest() {
    // given
    final CompletableActorFuture<Void> future = CompletableActorFuture.completed(null);
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  actor.runOnCompletionBlockingCurrentPhase(future, futureConsumer(TWO));
                  actions.add(ONE);
                });

            actor.run(runnable(THREE));
            actor.runOnCompletionBlockingCurrentPhase(future, futureConsumer(FOUR));
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // then
    // no guarantee of ordering between (ONE, THREE) and (TWO, FOUR), but the following constraints
    // must hold:
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, TWO));
    assertThat(actor.actions).containsSubsequence(newArrayList(THREE, TWO));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, FOUR));
    assertThat(actor.actions).containsSubsequence(newArrayList(THREE, FOUR));
  }

  @Test
  public void conditionTest() {
    // given
    final CompletableActorFuture<ActorCondition> conditionFuture = new CompletableActorFuture<>();
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  final ActorCondition condition =
                      actor.onCondition(
                          "cond",
                          () -> {
                            actions.add(THREE);
                            actor.run(
                                runnable(
                                    FOUR)); // this is done before the condition is fired for the
                            // second time
                          });
                  conditionFuture.complete(condition);
                  actions.add(ONE);
                });
            actor.run(runnable(TWO));
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    final ActorCondition condition = conditionFuture.join();
    condition.signal();
    condition.signal(); // condition is exactly once
    schedulerRule.workUntilDone();

    // then
    assertThat(actor.actions).containsSequence(newArrayList(THREE, FOUR, THREE, FOUR));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, THREE));
    assertThat(actor.actions).containsSubsequence(newArrayList(TWO, THREE));
  }

  @Test
  public void timerTest() {
    // given
    schedulerRule.getClock().setCurrentTime(100);
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  actor.runAtFixedRate(
                      Duration.ofMillis(10),
                      () -> {
                        actions.add(THREE);
                        actor.run(
                            runnable(
                                FOUR)); // this is done before the timer is fired for the second
                        // time
                      });
                  actions.add(ONE);
                });
            actor.run(runnable(TWO));
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();
    schedulerRule.getClock().addTime(Duration.ofMillis(10));
    schedulerRule.workUntilDone();
    schedulerRule.getClock().addTime(Duration.ofMillis(10));
    schedulerRule.workUntilDone();

    // then
    assertThat(actor.actions).containsSequence(newArrayList(THREE, FOUR, THREE, FOUR));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, THREE));
    assertThat(actor.actions).containsSubsequence(newArrayList(TWO, THREE));
  }

  @Test
  public void callTest() {
    // given
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  actor.runOnCompletion(
                      future,
                      (v, t) -> {
                        actions.add(THREE);
                        actor.run(runnable(FOUR));
                      });
                  actions.add(ONE);
                });
            actor.run(runnable(TWO));
          }
        };

    // when
    schedulerRule.submitActor(actor);

    actor
        .actorControl()
        .call(
            () -> {
              actor.actions.add(FIVE);
            });

    future.complete(null);

    schedulerRule.workUntilDone();

    // then
    assertThat(actor.actions).containsSequence(newArrayList(THREE, FOUR));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, THREE));
    assertThat(actor.actions).containsSubsequence(newArrayList(TWO, THREE));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, FIVE));
    assertThat(actor.actions).containsSubsequence(newArrayList(TWO, FIVE));
  }

  @Test
  public void callWithBlockingPhaseTest() {
    // given
    final CompletableActorFuture<Void> future = new CompletableActorFuture<>();
    final ActionRecordingActor actor =
        new ActionRecordingActor() {
          @Override
          protected void onActorStarted() {
            actor.run(
                () -> {
                  actor.runOnCompletionBlockingCurrentPhase(
                      future,
                      (v, t) -> {
                        actions.add(THREE);
                        actor.run(runnable(FOUR));
                      });
                  actions.add(ONE);
                });
            actor.run(runnable(TWO));
          }
        };

    // when
    schedulerRule.submitActor(actor);

    actor
        .actorControl()
        .call(
            () -> {
              actor.actions.add(FIVE);
            });

    future.complete(null);

    schedulerRule.workUntilDone();

    // then
    assertThat(actor.actions).containsSequence(newArrayList(THREE, FOUR));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, THREE));
    assertThat(actor.actions).containsSubsequence(newArrayList(TWO, THREE));
    assertThat(actor.actions).containsSubsequence(newArrayList(ONE, FIVE));
    assertThat(actor.actions).containsSubsequence(newArrayList(TWO, FIVE));
  }
}
