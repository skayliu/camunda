/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.dto.optimize.query.variable;

import org.camunda.optimize.dto.optimize.OptimizeDto;
import org.camunda.optimize.plugin.importing.variable.PluginVariableDto;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * This class is using the dto from the plugin system,
 * in order to enable to enrich the variable import.
 * <p>
 * Note: This class is still needed, because it implements
 * the optimize dto opposed to the plugin dto.
 */
public class ProcessVariableDto extends PluginVariableDto implements OptimizeDto {
  public ProcessVariableDto(final String id, final String name, final String type, final String value,
                            final OffsetDateTime timestamp, final Map<String, Object> valueInfo,
                            final String processDefinitionKey, final String processDefinitionId,
                            final String processInstanceId, final Long version, final String engineAlias,
                            final String tenantId) {
    super(
      id,
      name,
      type,
      value,
      timestamp,
      valueInfo,
      processDefinitionKey,
      processDefinitionId,
      processInstanceId,
      version,
      engineAlias,
      tenantId
    );
  }
}
