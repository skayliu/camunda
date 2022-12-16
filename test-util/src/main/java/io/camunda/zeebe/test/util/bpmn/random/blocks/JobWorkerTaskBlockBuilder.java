/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.test.util.bpmn.random.blocks;

import io.camunda.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.camunda.zeebe.model.bpmn.builder.AbstractJobWorkerTaskBuilder;
import io.camunda.zeebe.test.util.bpmn.random.BlockBuilder;
import io.camunda.zeebe.test.util.bpmn.random.BlockBuilderFactory;
import io.camunda.zeebe.test.util.bpmn.random.ConstructionContext;
import io.camunda.zeebe.test.util.bpmn.random.ExecutionPathContext;
import io.camunda.zeebe.test.util.bpmn.random.ExecutionPathSegment;
import io.camunda.zeebe.test.util.bpmn.random.RandomProcessGenerator;
import io.camunda.zeebe.test.util.bpmn.random.steps.AbstractExecutionStep;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepActivateAndCompleteJob;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepActivateAndFailJob;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepActivateAndTimeoutJob;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepActivateBPMNElement;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepActivateJobAndThrowError;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepTriggerTimerBoundaryEvent;
import java.util.Random;
import java.util.function.Function;

/**
 * Generates a task that is based on a job and is processed by a job worker (e.g. a service task).
 * The task may have boundary events
 */
public class JobWorkerTaskBlockBuilder extends AbstractBlockBuilder {

  private final String jobType;
  private final String errorCode;
  private final String boundaryErrorEventId;
  private final String boundaryTimerEventId;
  private final BoundaryEventBuilder boundaryEventBuilder;

  private final boolean hasBoundaryEvents;
  private final boolean hasBoundaryErrorEvent;
  private final boolean hasBoundaryTimerEvent;

  private final Function<AbstractFlowNodeBuilder<?, ?>, AbstractJobWorkerTaskBuilder<?, ?>>
      taskBuilder;

  public JobWorkerTaskBlockBuilder(
      final ConstructionContext context,
      final Function<AbstractFlowNodeBuilder<?, ?>, AbstractJobWorkerTaskBuilder<?, ?>>
          taskBuilder) {
    super(context.getIdGenerator().nextId());
    final Random random = context.getRandom();
    this.taskBuilder = taskBuilder;

    jobType = "job_" + elementId;
    errorCode = "error_" + elementId;

    boundaryErrorEventId = "boundary_error_" + elementId;
    boundaryTimerEventId = "boundary_timer_" + elementId;
    boundaryEventBuilder = new BoundaryEventBuilder(context, elementId);

    hasBoundaryErrorEvent =
        random.nextDouble() < RandomProcessGenerator.PROBABILITY_BOUNDARY_ERROR_EVENT;
    hasBoundaryTimerEvent =
        random.nextDouble() < RandomProcessGenerator.PROBABILITY_BOUNDARY_TIMER_EVENT;

    hasBoundaryEvents =
        hasBoundaryErrorEvent
            || hasBoundaryTimerEvent; // extend here for additional boundary events
  }

  @Override
  public AbstractFlowNodeBuilder<?, ?> buildFlowNodes(
      final AbstractFlowNodeBuilder<?, ?> nodeBuilder) {

    final var jobWorkerTaskBuilder = taskBuilder.apply(nodeBuilder);

    jobWorkerTaskBuilder.id(getElementId()).name(getElementId());
    jobWorkerTaskBuilder.zeebeJobRetries("3");
    jobWorkerTaskBuilder.zeebeJobType(jobType);

    AbstractFlowNodeBuilder<?, ?> result = jobWorkerTaskBuilder;

    if (hasBoundaryEvents) {
      if (hasBoundaryErrorEvent) {
        result =
            boundaryEventBuilder.connectBoundaryErrorEvent(
                jobWorkerTaskBuilder, errorCode, boundaryErrorEventId);
      }

      if (hasBoundaryTimerEvent) {
        result =
            boundaryEventBuilder.connectBoundaryTimerEvent(
                jobWorkerTaskBuilder, boundaryTimerEventId);
      }
    }

    return result;
  }

  /**
   * This generates a sequence of one or more steps. The final step is always a successful
   * activation and complete cycle. The steps before are randomly determined failed attempts.
   */
  @Override
  public ExecutionPathSegment generateRandomExecutionPath(final ExecutionPathContext context) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    final var activateStep = new StepActivateBPMNElement(getElementId());
    result.appendDirectSuccessor(activateStep);

    if (hasBoundaryTimerEvent) {
      // set an infinite timer as default; this can be overwritten by the execution path chosen
      result.setVariableDefault(
          boundaryTimerEventId, AbstractExecutionStep.VIRTUALLY_INFINITE.toString());
    }

    result.append(buildStepsForFailedExecutions(context.getRandom()));

    result.appendExecutionSuccessor(
        buildStepForSuccessfulExecution(context.getRandom(), result), activateStep);

    return result;
  }

  private ExecutionPathSegment buildStepsForFailedExecutions(final Random random) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    // If we already have a timer boundary event we should not timeout the job,
    // otherwise this makes the test too fragile (flaky).
    if (!hasBoundaryTimerEvent && random.nextBoolean()) {
      result.appendDirectSuccessor(new StepActivateAndTimeoutJob(jobType, getElementId()));
    }

    if (random.nextBoolean()) {
      final boolean updateRetries = random.nextBoolean();
      result.appendDirectSuccessor(
          new StepActivateAndFailJob(jobType, updateRetries, getElementId()));
    }

    return result;
  }

  /**
   * This method build the step that results in a successful execution of the service task.
   * Successful execution here does not necessarily mean that the job is completed orderly.
   * Successful execution is any execution which moves the token past the service task, so that the
   * process can continue.
   */
  private AbstractExecutionStep buildStepForSuccessfulExecution(
      final Random random, final ExecutionPathSegment executionPathSegment) {
    final AbstractExecutionStep result;

    if (hasBoundaryErrorEvent && random.nextBoolean()) {
      result = new StepActivateJobAndThrowError(jobType, errorCode, getElementId());
      executionPathSegment.setReachedTerminateEndEvent(
          boundaryEventBuilder.errorEventHasTerminateEndEvent());
    } else if (hasBoundaryTimerEvent && random.nextBoolean()) {
      result = new StepTriggerTimerBoundaryEvent(boundaryTimerEventId);
      executionPathSegment.setReachedTerminateEndEvent(
          boundaryEventBuilder.timerEventHasTerminateEndEvent());
    } else {
      result = new StepActivateAndCompleteJob(jobType, getElementId());
    }

    return result;
  }

  public static BlockBuilderFactory serviceTaskFactory() {
    return new Factory(AbstractFlowNodeBuilder::serviceTask);
  }

  public static BlockBuilderFactory businessRuleTaskFactory() {
    return new Factory(AbstractFlowNodeBuilder::businessRuleTask);
  }

  public static BlockBuilderFactory scriptTaskFactory() {
    return new Factory(AbstractFlowNodeBuilder::scriptTask);
  }

  public static BlockBuilderFactory sendTaskFactory() {
    return new Factory(AbstractFlowNodeBuilder::sendTask);
  }

  private static class Factory implements BlockBuilderFactory {

    private final Function<AbstractFlowNodeBuilder<?, ?>, AbstractJobWorkerTaskBuilder<?, ?>>
        taskBuilder;

    public Factory(
        final Function<AbstractFlowNodeBuilder<?, ?>, AbstractJobWorkerTaskBuilder<?, ?>>
            taskBuilder) {
      this.taskBuilder = taskBuilder;
    }

    @Override
    public BlockBuilder createBlockBuilder(final ConstructionContext context) {
      return new JobWorkerTaskBlockBuilder(context, taskBuilder);
    }

    @Override
    public boolean isAddingDepth() {
      return false;
    }
  }
}
