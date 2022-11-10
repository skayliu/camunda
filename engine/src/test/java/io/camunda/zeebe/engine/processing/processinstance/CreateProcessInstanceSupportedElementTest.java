/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.processinstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CreateProcessInstanceSupportedElementTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String PROCESS_ID = "processId";
  private static final String CHILD_PROCESS_ID = "childProcessId";
  private static final String START_ELEMENT_ID = "startElement";
  private static final String START_EVENT_ID = "startEvent";
  private static final String MESSAGE = "message";
  private static final String JOBTYPE = "jobtype";

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  private final Scenario scenario;

  public CreateProcessInstanceSupportedElementTest(final Scenario scenario) {
    this.scenario = scenario;
  }

  @Parameters(name = "{0}")
  public static Collection<Object> scenarios() {
    return List.of(
        new Scenario(
            BpmnElementType.SUB_PROCESS,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent()
                .subProcess(START_ELEMENT_ID)
                .embeddedSubProcess()
                .startEvent(START_EVENT_ID)
                .subProcessDone()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.EVENT_SUB_PROCESS,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .eventSubProcess(
                    START_ELEMENT_ID,
                    e -> e.startEvent(START_EVENT_ID).timerWithDuration("PT1H").endEvent())
                .startEvent()
                .endEvent()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.INTERMEDIATE_CATCH_EVENT,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .intermediateCatchEvent(START_ELEMENT_ID)
                .message(b -> b.name(MESSAGE).zeebeCorrelationKeyExpression("correlationKey"))
                .done(),
            Map.of("correlationKey", "value")),
        new Scenario(
            BpmnElementType.INTERMEDIATE_THROW_EVENT,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .intermediateThrowEvent(START_ELEMENT_ID)
                .endEvent()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.END_EVENT,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .endEvent(START_ELEMENT_ID)
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.SERVICE_TASK,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .serviceTask(START_ELEMENT_ID, b -> b.zeebeJobType(JOBTYPE))
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.RECEIVE_TASK,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .receiveTask(START_ELEMENT_ID)
                .message(b -> b.name(MESSAGE).zeebeCorrelationKeyExpression("correlationKey"))
                .done(),
            Map.of("correlationKey", "value")),
        new Scenario(
            BpmnElementType.USER_TASK,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .userTask(START_ELEMENT_ID)
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.MANUAL_TASK,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .manualTask(START_ELEMENT_ID)
                .endEvent()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.EXCLUSIVE_GATEWAY,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .exclusiveGateway(START_ELEMENT_ID)
                .defaultFlow()
                .endEvent()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.PARALLEL_GATEWAY,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .parallelGateway(START_ELEMENT_ID)
                .endEvent()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.EVENT_BASED_GATEWAY,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .eventBasedGateway(START_ELEMENT_ID)
                .intermediateCatchEvent()
                .message(b -> b.name(MESSAGE).zeebeCorrelationKeyExpression("correlationKey"))
                .moveToLastGateway()
                .intermediateCatchEvent()
                .timerWithDuration("PT1H")
                .done(),
            Map.of("correlationKey", "value")),
        new Scenario(
            BpmnElementType.MULTI_INSTANCE_BODY,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .serviceTask(
                    START_ELEMENT_ID,
                    t ->
                        t.zeebeJobType(JOBTYPE)
                            .multiInstance(m -> m.parallel().zeebeInputCollectionExpression("[1]")))
                .endEvent()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.CALL_ACTIVITY,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .callActivity(START_ELEMENT_ID, c -> c.zeebeProcessId(CHILD_PROCESS_ID))
                .endEvent()
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.BUSINESS_RULE_TASK,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .businessRuleTask(START_ELEMENT_ID, b -> b.zeebeJobType(JOBTYPE))
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.SCRIPT_TASK,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .scriptTask(START_ELEMENT_ID, b -> b.zeebeJobType(JOBTYPE))
                .done(),
            Collections.emptyMap()),
        new Scenario(
            BpmnElementType.SEND_TASK,
            Bpmn.createExecutableProcess(PROCESS_ID)
                .startEvent(START_EVENT_ID)
                .sendTask(START_ELEMENT_ID, b -> b.zeebeJobType(JOBTYPE))
                .done(),
            Collections.emptyMap()));
  }

  @Test
  public void testProcessInstanceCanStartAtElementType() {
    // given
    ENGINE.deployment().withXmlResource(scenario.modelInstance).deploy();
    if (scenario.type == BpmnElementType.CALL_ACTIVITY) {
      ENGINE.deployment().withXmlResource(getChildProcess()).deploy();
    }

    // when
    final long instanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId(PROCESS_ID)
            .withStartInstruction(START_ELEMENT_ID)
            .withVariables(scenario.variables)
            .create();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(instanceKey)
                .onlyEvents()
                .limit(
                    r ->
                        r.getValue().getBpmnElementType() == scenario.type
                            && r.getIntent() == ProcessInstanceIntent.ELEMENT_ACTIVATED))
        .extracting(record -> record.getValue().getBpmnElementType(), Record::getIntent)
        .containsSequence(
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            tuple(scenario.type, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(scenario.type, ProcessInstanceIntent.ELEMENT_ACTIVATED));
  }

  @Test
  public void testProcessInstanceCanStartAtStartEvent() {
    // given
    ENGINE.deployment().withXmlResource(scenario.modelInstance).deploy();
    if (scenario.type == BpmnElementType.CALL_ACTIVITY) {
      ENGINE.deployment().withXmlResource(getChildProcess()).deploy();
    }

    // when
    final long instanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId(PROCESS_ID)
            .withStartInstruction(START_EVENT_ID)
            .withVariables(scenario.variables)
            .create();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(instanceKey)
                .onlyEvents()
                .limit(
                    r ->
                        r.getValue().getElementId().equals(START_EVENT_ID)
                            && r.getIntent() == ProcessInstanceIntent.ELEMENT_ACTIVATED))
        .extracting(
            record -> record.getValue().getElementId(),
            record -> record.getValue().getBpmnElementType(),
            Record::getIntent)
        .containsSubsequence(
            tuple(PROCESS_ID, BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(PROCESS_ID, BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            tuple(
                START_EVENT_ID,
                BpmnElementType.START_EVENT,
                ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(
                START_EVENT_ID,
                BpmnElementType.START_EVENT,
                ProcessInstanceIntent.ELEMENT_ACTIVATED));
  }

  private BpmnModelInstance getChildProcess() {
    return Bpmn.createExecutableProcess(CHILD_PROCESS_ID).startEvent().endEvent().done();
  }

  record Scenario(
      BpmnElementType type, BpmnModelInstance modelInstance, Map<String, Object> variables) {}
}
