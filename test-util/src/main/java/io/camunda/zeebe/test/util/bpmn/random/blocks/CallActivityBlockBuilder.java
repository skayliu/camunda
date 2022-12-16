/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.test.util.bpmn.random.blocks;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.camunda.zeebe.test.util.bpmn.random.BlockBuilder;
import io.camunda.zeebe.test.util.bpmn.random.BlockBuilderFactory;
import io.camunda.zeebe.test.util.bpmn.random.ConstructionContext;
import io.camunda.zeebe.test.util.bpmn.random.ExecutionPathContext;
import io.camunda.zeebe.test.util.bpmn.random.ExecutionPathSegment;
import java.util.Random;

/** Generates a call activity. The called process is a Process that contains any block sequence. */
public class CallActivityBlockBuilder extends AbstractBlockBuilder {

  private final boolean shouldPropagateAllChildVariables;
  private final String calledProcessId;
  private final BlockSequenceBuilder calledProcessBuilder;
  private final ConstructionContext context;

  public CallActivityBlockBuilder(final ConstructionContext context) {
    super("call_activity_", context.getIdGenerator().nextId());
    final Random random = context.getRandom();
    final int maxDepth = context.getMaxDepth();
    final int currentDepth = context.getCurrentDepth();
    final boolean goDeeper = random.nextInt(maxDepth) > currentDepth;

    this.context = context;
    shouldPropagateAllChildVariables = random.nextBoolean();

    calledProcessId = "process_child_" + elementId;

    if (goDeeper) {
      calledProcessBuilder =
          context
              .getBlockSequenceBuilderFactory()
              .createBlockSequenceBuilder(context.withIncrementedDepth());
    } else {
      calledProcessBuilder = null;
    }
  }

  @Override
  public AbstractFlowNodeBuilder<?, ?> buildFlowNodes(
      final AbstractFlowNodeBuilder<?, ?> nodeBuilder) {

    buildChildProcess();

    return nodeBuilder
        .callActivity(getElementId())
        .zeebeProcessId(calledProcessId)
        .zeebePropagateAllChildVariables(shouldPropagateAllChildVariables);
  }

  @Override
  public ExecutionPathSegment generateRandomExecutionPath(final ExecutionPathContext context) {
    final ExecutionPathSegment result = new ExecutionPathSegment();

    if (calledProcessBuilder != null) {
      final var calledProcessExecutionPath = calledProcessBuilder.findRandomExecutionPath(context);
      final var isDifferentFlowScope = true;
      result.append(calledProcessExecutionPath, isDifferentFlowScope);
    }

    return result;
  }

  private void buildChildProcess() {
    AbstractFlowNodeBuilder<?, ?> workInProgress =
        Bpmn.createExecutableProcess(calledProcessId).startEvent();

    if (calledProcessBuilder != null) {
      workInProgress = calledProcessBuilder.buildFlowNodes(workInProgress);
    }

    final BpmnModelInstance childModelInstance = workInProgress.endEvent().done();
    context.addCalledChildProcess(childModelInstance);
  }

  public static class Factory implements BlockBuilderFactory {

    @Override
    public BlockBuilder createBlockBuilder(final ConstructionContext context) {
      return new CallActivityBlockBuilder(context);
    }

    @Override
    public boolean isAddingDepth() {
      return true;
    }
  }
}
