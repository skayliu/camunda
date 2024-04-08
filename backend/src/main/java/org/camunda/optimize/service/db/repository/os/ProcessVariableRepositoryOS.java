/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.service.db.repository.os;

import static org.camunda.optimize.service.db.DatabaseConstants.NUMBER_OF_RETRIES_ON_CONFLICT;
import static org.camunda.optimize.service.db.os.externalcode.client.dsl.QueryDSL.script;
import static org.camunda.optimize.service.util.InstanceIndexUtil.getProcessInstanceIndexAliasName;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.camunda.optimize.dto.optimize.DecisionDefinitionOptimizeDto;
import org.camunda.optimize.service.db.os.OptimizeOpenSearchClient;
import org.camunda.optimize.service.db.repository.ProcessVariableRepository;
import org.camunda.optimize.service.db.repository.script.ProcessInstanceScriptFactory;
import org.camunda.optimize.service.db.schema.OptimizeIndexNameService;
import org.camunda.optimize.service.util.configuration.condition.OpenSearchCondition;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
@Conditional(OpenSearchCondition.class)
public class ProcessVariableRepositoryOS implements ProcessVariableRepository {
  private final OptimizeOpenSearchClient osClient;
  private final OptimizeIndexNameService indexNameService;

  @Override
  public void deleteVariableDataByProcessInstanceIds(
      final String processDefinitionKey, final List<String> processInstanceIds) {
    final List<BulkOperation> bulkOperations =
        processInstanceIds.stream()
            .map(
                processInstanceId -> {
                  final UpdateOperation<DecisionDefinitionOptimizeDto> updateOperation =
                      new UpdateOperation.Builder<DecisionDefinitionOptimizeDto>()
                          .index(
                              indexNameService.getOptimizeIndexAliasForIndex(
                                  getProcessInstanceIndexAliasName(processDefinitionKey)))
                          .id(processInstanceId)
                          .script(
                              script(
                                  ProcessInstanceScriptFactory.createVariableClearScript(),
                                  Map.of()))
                          .retryOnConflict(NUMBER_OF_RETRIES_ON_CONFLICT)
                          .build();
                  return new BulkOperation.Builder().update(updateOperation).build();
                })
            .toList();

    osClient.doBulkRequest(
        () -> new BulkRequest.Builder().refresh(Refresh.True),
        bulkOperations,
        getProcessInstanceIndexAliasName(processDefinitionKey),
        false);
  }
}
