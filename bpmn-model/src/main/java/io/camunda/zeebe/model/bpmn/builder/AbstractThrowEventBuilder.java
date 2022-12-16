/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.camunda.zeebe.model.bpmn.builder;

import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.CompensateEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.EscalationEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.LinkEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.MessageEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.SignalEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.ThrowEvent;

/**
 * @author Sebastian Menski
 */
public abstract class AbstractThrowEventBuilder<
        B extends AbstractThrowEventBuilder<B, E>, E extends ThrowEvent>
    extends AbstractEventBuilder<B, E> implements ZeebeJobWorkerElementBuilder<B> {

  private final ZeebeJobWorkerPropertiesBuilder<B> jobWorkerPropertiesBuilder;
  private final ZeebeVariablesMappingBuilder<B> variablesMappingBuilder;

  protected AbstractThrowEventBuilder(
      final BpmnModelInstance modelInstance, final E element, final Class<?> selfType) {
    super(modelInstance, element, selfType);
    jobWorkerPropertiesBuilder = new ZeebeJobWorkerPropertiesBuilderImpl<>(myself);
    variablesMappingBuilder = new ZeebeVariableMappingBuilderImpl<>(myself);
  }

  /**
   * Sets an event definition for the given message name. If already a message with this name exists
   * it will be used, otherwise a new message is created.
   *
   * @param messageName the name of the message
   * @return the builder object
   */
  public B message(final String messageName) {
    final MessageEventDefinition messageEventDefinition = createMessageEventDefinition(messageName);
    element.getEventDefinitions().add(messageEventDefinition);

    return myself;
  }

  /**
   * Creates an empty message event definition with an unique id and returns a builder for the
   * message event definition.
   *
   * @return the message event definition builder object
   */
  public MessageEventDefinitionBuilder messageEventDefinition() {
    return messageEventDefinition(null);
  }

  /**
   * Creates an empty message event definition with the given id and returns a builder for the
   * message event definition.
   *
   * @param id the id of the message event definition
   * @return the message event definition builder object
   */
  public MessageEventDefinitionBuilder messageEventDefinition(final String id) {
    final MessageEventDefinition messageEventDefinition = createEmptyMessageEventDefinition();
    if (id != null) {
      messageEventDefinition.setId(id);
    }

    element.getEventDefinitions().add(messageEventDefinition);
    return new MessageEventDefinitionBuilder(modelInstance, messageEventDefinition);
  }

  /**
   * Sets an event definition for the given signal name. If already a signal with this name exists
   * it will be used, otherwise a new signal is created.
   *
   * @param signalName the name of the signal
   * @return the builder object
   */
  public B signal(final String signalName) {
    final SignalEventDefinition signalEventDefinition = createSignalEventDefinition(signalName);
    element.getEventDefinitions().add(signalEventDefinition);

    return myself;
  }

  /**
   * Creates an empty signal event definition with an unique id and returns a builder for the signal
   * event definition.
   *
   * @return the signal event definition builder object
   */
  public SignalEventDefinitionBuilder signalEventDefinition() {
    final SignalEventDefinition eventDefinition = createEmptySignalEventDefinition();
    element.getEventDefinitions().add(eventDefinition);
    return new SignalEventDefinitionBuilder(modelInstance, eventDefinition);
  }

  /**
   * Sets an event definition for the given Signal name. If a signal with this name already exists
   * it will be used, otherwise a new signal is created. It returns a builder for the Signal Event
   * Definition.
   *
   * @param signalName the name of the signal
   * @return the signal event definition builder object
   */
  public SignalEventDefinitionBuilder signalEventDefinition(final String signalName) {
    final SignalEventDefinition signalEventDefinition = createSignalEventDefinition(signalName);
    element.getEventDefinitions().add(signalEventDefinition);

    return new SignalEventDefinitionBuilder(modelInstance, signalEventDefinition);
  }

  /**
   * Sets a dynamic escalation code for the escalation that is retrieved from the given expression
   *
   * @param escalationCodeExpression the expression for the escalation
   * @return the builder object
   */
  public B escalationExpression(final String escalationCodeExpression) {
    return escalation(asZeebeExpression(escalationCodeExpression));
  }

  /**
   * Sets an escalation definition for the given escalation code. If already an escalation with this
   * code exists it will be used, otherwise a new escalation is created.
   *
   * @param escalationCode the code of the escalation
   * @return the builder object
   */
  public B escalation(final String escalationCode) {
    final EscalationEventDefinition escalationEventDefinition =
        createEscalationEventDefinition(escalationCode);
    element.getEventDefinitions().add(escalationEventDefinition);

    return myself;
  }

  /**
   * Creates an escalation event definition with an unique id and returns a builder for the
   * escalation event definition.
   *
   * @return the escalation event definition builder object
   */
  public EscalationEventDefinitionBuilder escalationEventDefinition(final String id) {
    final EscalationEventDefinition escalationEventDefinition =
        createEmptyEscalationEventDefinition();
    if (id != null) {
      escalationEventDefinition.setId(id);
    }

    element.getEventDefinitions().add(escalationEventDefinition);
    return new EscalationEventDefinitionBuilder(modelInstance, escalationEventDefinition);
  }

  /**
   * Creates an escalation event definition and returns a builder for the escalation event
   * definition.
   *
   * @return the escalation event definition builder object
   */
  public EscalationEventDefinitionBuilder escalationEventDefinition() {
    final EscalationEventDefinition escalationEventDefinition =
        createEmptyEscalationEventDefinition();
    element.getEventDefinitions().add(escalationEventDefinition);
    return new EscalationEventDefinitionBuilder(modelInstance, escalationEventDefinition);
  }

  public CompensateEventDefinitionBuilder compensateEventDefinition() {
    return compensateEventDefinition(null);
  }

  public CompensateEventDefinitionBuilder compensateEventDefinition(final String id) {
    final CompensateEventDefinition eventDefinition =
        createInstance(CompensateEventDefinition.class);
    if (id != null) {
      eventDefinition.setId(id);
    }

    element.getEventDefinitions().add(eventDefinition);
    return new CompensateEventDefinitionBuilder(modelInstance, eventDefinition);
  }

  /**
   * Sets a link event definition for the given link name.
   *
   * @param linkName the name of the link
   * @return the builder object
   */
  public B link(final String linkName) {
    linkEventDefinition().name(linkName);
    return myself;
  }

  /**
   * Creates an empty link event definition with a unique id and returns a builder for the link
   * event definition.
   *
   * @return the link event definition builder object
   */
  public LinkEventDefinitionBuilder linkEventDefinition() {
    return linkEventDefinition(null);
  }

  /**
   * Creates an empty link event definition with the given id and returns a builder for the link
   * event definition.
   *
   * @param id the id of the link event definition
   * @return the link event definition builder object
   */
  public LinkEventDefinitionBuilder linkEventDefinition(final String id) {
    final LinkEventDefinition eventDefinition = createInstance(LinkEventDefinition.class);
    if (id != null) {
      eventDefinition.setId(id);
    }

    element.getEventDefinitions().add(eventDefinition);
    return new LinkEventDefinitionBuilder(modelInstance, eventDefinition);
  }

  @Override
  public B zeebeJobType(final String type) {
    return jobWorkerPropertiesBuilder.zeebeJobType(type);
  }

  @Override
  public B zeebeJobTypeExpression(final String expression) {
    return jobWorkerPropertiesBuilder.zeebeJobTypeExpression(expression);
  }

  @Override
  public B zeebeJobRetries(final String retries) {
    return jobWorkerPropertiesBuilder.zeebeJobRetries(retries);
  }

  @Override
  public B zeebeJobRetriesExpression(final String expression) {
    return jobWorkerPropertiesBuilder.zeebeJobRetriesExpression(expression);
  }

  @Override
  public B zeebeTaskHeader(final String key, final String value) {
    return jobWorkerPropertiesBuilder.zeebeTaskHeader(key, value);
  }

  @Override
  public B zeebeInputExpression(final String sourceExpression, final String target) {
    return variablesMappingBuilder.zeebeInputExpression(sourceExpression, target);
  }

  @Override
  public B zeebeOutputExpression(final String sourceExpression, final String target) {
    return variablesMappingBuilder.zeebeOutputExpression(sourceExpression, target);
  }

  @Override
  public B zeebeInput(final String source, final String target) {
    return variablesMappingBuilder.zeebeInput(source, target);
  }

  @Override
  public B zeebeOutput(final String source, final String target) {
    return variablesMappingBuilder.zeebeOutput(source, target);
  }
}
