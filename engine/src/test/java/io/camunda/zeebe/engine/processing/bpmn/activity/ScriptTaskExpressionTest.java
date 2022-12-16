/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.bpmn.activity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.builder.ScriptTaskBuilder;
import io.camunda.zeebe.protocol.record.Assertions;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.intent.VariableIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.ErrorType;
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.camunda.zeebe.protocol.record.value.VariableRecordValue;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class ScriptTaskExpressionTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  private static final String PROCESS_ID = "process";
  private static final String TASK_ID = "task";
  private static final String RESULT_VARIABLE = "result";
  private static final String OUTPUT_TARGET = "output";

  private static final String A_STRING = "foobar";
  private static final String A_SUB_STRING = "\"bar\"";

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  private static BpmnModelInstance processWithScriptTask(
      final Consumer<ScriptTaskBuilder> modifier) {
    return Bpmn.createExecutableProcess(PROCESS_ID)
        .startEvent()
        .scriptTask(TASK_ID, modifier)
        .endEvent()
        .done();
  }

  @Test
  public void shouldActivateTask() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            processWithScriptTask(
                t -> t.zeebeExpression("substring(x, 4)").zeebeResultVariable(RESULT_VARIABLE)))
        .deploy();

    // when
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).withVariable("x", A_STRING).create();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .withElementType(BpmnElementType.SCRIPT_TASK)
                .limit(3))
        .extracting(Record::getRecordType, Record::getIntent)
        .containsSequence(
            tuple(RecordType.COMMAND, ProcessInstanceIntent.ACTIVATE_ELEMENT),
            tuple(RecordType.EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(RecordType.EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATED));

    final Record<ProcessInstanceRecordValue> taskActivating =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATING)
            .withElementType(BpmnElementType.SCRIPT_TASK)
            .getFirst();

    Assertions.assertThat(taskActivating.getValue())
        .hasElementId(TASK_ID)
        .hasBpmnElementType(BpmnElementType.SCRIPT_TASK)
        .hasFlowScopeKey(processInstanceKey)
        .hasBpmnProcessId(PROCESS_ID)
        .hasProcessInstanceKey(processInstanceKey);
  }

  @Test
  public void shouldCompleteTask() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            processWithScriptTask(
                t -> t.zeebeExpression("substring(x, 4)").zeebeResultVariable(RESULT_VARIABLE)))
        .deploy();

    // when
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).withVariable("x", A_STRING).create();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(r -> r.getValue().getBpmnElementType(), Record::getIntent)
        .containsSubsequence(
            tuple(BpmnElementType.SCRIPT_TASK, ProcessInstanceIntent.ELEMENT_COMPLETING),
            tuple(BpmnElementType.SCRIPT_TASK, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED));
  }

  @Test
  public void shouldWriteResultAsProcessInstanceVariable() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            processWithScriptTask(
                t -> t.zeebeExpression("substring(x, 4)").zeebeResultVariable(RESULT_VARIABLE)))
        .deploy();

    // when
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).withVariable("x", A_STRING).create();

    // then
    assertThat(
            RecordingExporter.variableRecords(VariableIntent.CREATED)
                .withProcessInstanceKey(processInstanceKey)
                .withName(RESULT_VARIABLE)
                .getFirst())
        .extracting(Record::getValue)
        .extracting(VariableRecordValue::getScopeKey, VariableRecordValue::getValue)
        .containsExactly(processInstanceKey, A_SUB_STRING);
  }

  @Test
  public void shouldUseResultInOutputMappings() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            processWithScriptTask(
                t ->
                    t.zeebeExpression("substring(x, 4)")
                        .zeebeResultVariable(RESULT_VARIABLE)
                        .zeebeOutputExpression(RESULT_VARIABLE, OUTPUT_TARGET)))
        .deploy();

    // when
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).withVariable("x", A_STRING).create();

    final long taskInstanceKey =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withProcessInstanceKey(processInstanceKey)
            .withElementType(BpmnElementType.SCRIPT_TASK)
            .getFirst()
            .getKey();

    // then
    assertThat(
            RecordingExporter.variableRecords(VariableIntent.CREATED)
                .withProcessInstanceKey(processInstanceKey)
                .withName(RESULT_VARIABLE)
                .getFirst())
        .extracting(Record::getValue)
        .extracting(VariableRecordValue::getScopeKey, VariableRecordValue::getValue)
        .containsExactly(taskInstanceKey, A_SUB_STRING);

    assertThat(
            RecordingExporter.variableRecords(VariableIntent.CREATED)
                .withProcessInstanceKey(processInstanceKey)
                .withName(OUTPUT_TARGET)
                .getFirst())
        .extracting(Record::getValue)
        .extracting(VariableRecordValue::getScopeKey, VariableRecordValue::getValue)
        .containsExactly(processInstanceKey, A_SUB_STRING);
  }

  @Test
  public void shouldCreateIncidentIfScriptExpressionEvaluationFailed() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            processWithScriptTask(
                t ->
                    t.zeebeExpression("x")
                        .zeebeResultVariable(RESULT_VARIABLE)
                        .zeebeOutputExpression(RESULT_VARIABLE, OUTPUT_TARGET)))
        .deploy();

    // when
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    // then
    final var scriptTask =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATING)
            .withProcessInstanceKey(processInstanceKey)
            .withElementId(TASK_ID)
            .withElementType(BpmnElementType.SCRIPT_TASK)
            .getFirst();

    final var incidentRecord =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    Assertions.assertThat(incidentRecord.getValue())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage("failed to evaluate expression 'x': no variable found for name 'x'")
        .hasBpmnProcessId(scriptTask.getValue().getBpmnProcessId())
        .hasProcessDefinitionKey(scriptTask.getValue().getProcessDefinitionKey())
        .hasProcessInstanceKey(scriptTask.getValue().getProcessInstanceKey())
        .hasElementId(scriptTask.getValue().getElementId())
        .hasElementInstanceKey(scriptTask.getKey())
        .hasVariableScopeKey(scriptTask.getKey())
        .hasJobKey(-1);
  }

  @Test
  public void shouldResolveIncidentIfScriptExpressionEvaluationFailed() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            processWithScriptTask(
                t ->
                    t.zeebeExpression("x")
                        .zeebeResultVariable(RESULT_VARIABLE)
                        .zeebeOutputExpression(RESULT_VARIABLE, OUTPUT_TARGET)))
        .deploy();

    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    final Record<IncidentRecordValue> incidentCreatedRecord =
        RecordingExporter.incidentRecords(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    ENGINE
        .variables()
        .ofScope(incidentCreatedRecord.getValue().getElementInstanceKey())
        .withDocument(Map.of("x", A_STRING))
        .update();

    // when
    final Record<IncidentRecordValue> incidentResolvedEvent =
        ENGINE
            .incident()
            .ofInstance(processInstanceKey)
            .withKey(incidentCreatedRecord.getKey())
            .resolve();

    // then
    Assertions.assertThat(
            RecordingExporter.variableRecords(VariableIntent.CREATED)
                .withProcessInstanceKey(processInstanceKey)
                .withName(OUTPUT_TARGET)
                .getFirst()
                .getValue())
        .hasValue("\"foobar\"");

    assertThat(incidentResolvedEvent.getKey()).isEqualTo(incidentCreatedRecord.getKey());
  }
}
