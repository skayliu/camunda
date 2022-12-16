/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.test.util.bpmn.random.blocks;

import io.camunda.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.camunda.zeebe.model.bpmn.builder.SubProcessBuilder;
import io.camunda.zeebe.test.util.bpmn.random.BlockBuilder;
import io.camunda.zeebe.test.util.bpmn.random.BlockBuilderFactory;
import io.camunda.zeebe.test.util.bpmn.random.ConstructionContext;
import io.camunda.zeebe.test.util.bpmn.random.ExecutionPathContext;
import io.camunda.zeebe.test.util.bpmn.random.ExecutionPathSegment;
import io.camunda.zeebe.test.util.bpmn.random.IDGenerator;
import io.camunda.zeebe.test.util.bpmn.random.RandomProcessGenerator;
import io.camunda.zeebe.test.util.bpmn.random.steps.AbstractExecutionStep;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepActivateBPMNElement;
import io.camunda.zeebe.test.util.bpmn.random.steps.StepTriggerTimerBoundaryEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates an embedded sub process. The embedded sub process contains either a sequence of random
 * blocks or a start event directly connected to the end event
 */
public class SubProcessBlockBuilder extends AbstractBlockBuilder {

  private BlockBuilder embeddedSubProcessBuilder;
  private final String subProcessStartEventId;
  private final String subProcessEndEventId;
  private final String subProcessBoundaryTimerEventId;
  private final BoundaryEventBuilder boundaryEventBuilder;

  private final boolean hasBoundaryEvents;
  private final boolean hasBoundaryTimerEvent;

  public SubProcessBlockBuilder(final ConstructionContext context) {
    super(context.getIdGenerator().nextId());
    final Random random = context.getRandom();
    final IDGenerator idGenerator = context.getIdGenerator();
    final BlockSequenceBuilder.BlockSequenceBuilderFactory factory =
        context.getBlockSequenceBuilderFactory();
    final int maxDepth = context.getMaxDepth();
    final int currentDepth = context.getCurrentDepth();

    subProcessStartEventId = idGenerator.nextId();
    subProcessEndEventId = idGenerator.nextId();

    subProcessBoundaryTimerEventId = "boundary_timer_" + elementId;
    boundaryEventBuilder = new BoundaryEventBuilder(context, elementId);

    final boolean goDeeper = random.nextInt(maxDepth) > currentDepth;

    if (goDeeper) {
      embeddedSubProcessBuilder =
          factory.createBlockSequenceBuilder(context.withIncrementedDepth());
      hasBoundaryTimerEvent =
          random.nextDouble() < RandomProcessGenerator.PROBABILITY_BOUNDARY_TIMER_EVENT;
    } else {
      hasBoundaryTimerEvent = false;
    }

    hasBoundaryEvents = hasBoundaryTimerEvent; // extend here
  }

  @Override
  public AbstractFlowNodeBuilder<?, ?> buildFlowNodes(
      final AbstractFlowNodeBuilder<?, ?> nodeBuilder) {
    final SubProcessBuilder subProcessBuilderStart = nodeBuilder.subProcess(getElementId());

    AbstractFlowNodeBuilder<?, ?> workInProgress =
        subProcessBuilderStart.embeddedSubProcess().startEvent(subProcessStartEventId);

    if (embeddedSubProcessBuilder != null) {
      workInProgress = embeddedSubProcessBuilder.buildFlowNodes(workInProgress);
    }

    final var subProcessBuilderDone =
        workInProgress.endEvent(subProcessEndEventId).subProcessDone();

    AbstractFlowNodeBuilder result = subProcessBuilderDone;
    if (hasBoundaryEvents) {
      if (hasBoundaryTimerEvent) {
        result =
            boundaryEventBuilder.connectBoundaryTimerEvent(
                subProcessBuilderDone, subProcessBoundaryTimerEventId);
      }
    }

    return result;
  }

  @Override
  public ExecutionPathSegment generateRandomExecutionPath(final ExecutionPathContext context) {
    final ExecutionPathSegment result = new ExecutionPathSegment();
    final Random random = context.getRandom();

    if (hasBoundaryTimerEvent) {
      // set an infinite timer as default; this can be overwritten by the execution path chosen
      result.setVariableDefault(
          subProcessBoundaryTimerEventId, AbstractExecutionStep.VIRTUALLY_INFINITE.toString());
    }
    final var activateSubProcess = new StepActivateBPMNElement(getElementId());

    result.appendDirectSuccessor(activateSubProcess);

    if (embeddedSubProcessBuilder == null) {
      return result;
    }

    final var internalExecutionPath = embeddedSubProcessBuilder.findRandomExecutionPath(context);

    if (!hasBoundaryEvents || !internalExecutionPath.canBeInterrupted() || random.nextBoolean()) {
      final var isDifferentFlowScope = true;
      result.append(internalExecutionPath, isDifferentFlowScope);
    } else {
      internalExecutionPath.cutAtRandomPosition(random);
      final var isDifferentFlowScope = true;
      result.append(internalExecutionPath, isDifferentFlowScope);
      if (hasBoundaryTimerEvent) {
        result.appendExecutionSuccessor(
            new StepTriggerTimerBoundaryEvent(subProcessBoundaryTimerEventId), activateSubProcess);
        result.setReachedTerminateEndEvent(boundaryEventBuilder.timerEventHasTerminateEndEvent());
      } // extend here for other boundary events
    }

    return result;
  }

  @Override
  public List<BlockBuilder> getPossibleStartingBlocks() {
    final List<BlockBuilder> blockBuilders = new ArrayList<>();
    blockBuilders.add(this);
    if (embeddedSubProcessBuilder != null) {
      blockBuilders.addAll(embeddedSubProcessBuilder.getPossibleStartingBlocks());
    }
    return blockBuilders;
  }

  public static class Factory implements BlockBuilderFactory {

    @Override
    public BlockBuilder createBlockBuilder(final ConstructionContext context) {
      return new SubProcessBlockBuilder(context);
    }

    @Override
    public boolean isAddingDepth() {
      return true;
    }
  }
}
