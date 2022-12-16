/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.testing;

import io.camunda.zeebe.scheduler.ActorThread;
import io.camunda.zeebe.scheduler.ActorThreadGroup;
import io.camunda.zeebe.scheduler.ActorTimerQueue;
import io.camunda.zeebe.scheduler.TaskScheduler;
import io.camunda.zeebe.scheduler.clock.ActorClock;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.agrona.LangUtil;

public final class ControlledActorThread extends ActorThread {
  private final CyclicBarrier barrier = new CyclicBarrier(2);

  public ControlledActorThread(
      final String name,
      final int id,
      final ActorThreadGroup threadGroup,
      final TaskScheduler taskScheduler,
      final ActorClock clock,
      final ActorTimerQueue timerQueue) {
    super(name, id, threadGroup, taskScheduler, clock, timerQueue, false);
    idleStrategy = new ControlledIdleStartegy();
  }

  public void resumeTasks() {
    try {
      barrier.await(); // work at least 1 full cycle until the runner becomes idle after having been
    } catch (final InterruptedException | BrokenBarrierException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  // ControlledActorThread#resumeTasks must be called before this method
  public void waitUntilDone() {
    while (barrier.getNumberWaiting() < 1) {
      // spin until thread is idle again
      Thread.yield();
    }
  }

  public void workUntilDone() {
    resumeTasks();
    waitUntilDone();
  }

  class ControlledIdleStartegy extends ActorTaskRunnerIdleStrategy {
    @Override
    protected void onIdle() {
      super.onIdle();

      try {
        barrier.await();
      } catch (final InterruptedException | BrokenBarrierException e) {
        LangUtil.rethrowUnchecked(e);
      }
    }
  }
}
