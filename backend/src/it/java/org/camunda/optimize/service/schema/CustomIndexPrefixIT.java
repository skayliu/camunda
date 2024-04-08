/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.optimize.service.db.DatabaseConstants.PROCESS_INSTANCE_MULTI_ALIAS;
import static org.camunda.optimize.service.db.schema.OptimizeIndexNameService.getOptimizeIndexAliasForIndexNameAndPrefix;
import static org.camunda.optimize.service.db.schema.OptimizeIndexNameService.getOptimizeIndexOrTemplateNameForAliasAndVersion;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.camunda.optimize.AbstractPlatformIT;
import org.camunda.optimize.service.db.DatabaseClient;
import org.camunda.optimize.service.db.es.OptimizeElasticsearchClient;
import org.camunda.optimize.service.db.schema.IndexMappingCreator;
import org.camunda.optimize.test.it.extension.DatabaseIntegrationTestExtension;
import org.camunda.optimize.util.BpmnModels;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class CustomIndexPrefixIT extends AbstractPlatformIT {
  private static final String CUSTOM_PREFIX = UUID.randomUUID().toString().substring(0, 5);

  @RegisterExtension
  @Order(2)
  public DatabaseIntegrationTestExtension customPrefixDatabaseIntegrationTestExtension =
      new DatabaseIntegrationTestExtension(CUSTOM_PREFIX);

  private DatabaseClient prefixAwareDatabaseClient;

  @BeforeEach
  public void setUp() {
    prefixAwareDatabaseClient = embeddedOptimizeExtension.getOptimizeDatabaseClient();
  }

  @Test
  public void optimizeCustomPrefixIndexExistsAfterSchemaInitialization() {
    // given
    embeddedOptimizeExtension
        .getConfigurationService()
        .getElasticSearchConfiguration()
        .setIndexPrefix(CUSTOM_PREFIX);
    embeddedOptimizeExtension.reloadConfiguration();

    // when
    initializeSchema();

    // then
    assertThat(prefixAwareDatabaseClient.getIndexNameService().getIndexPrefix())
        .isEqualTo(CUSTOM_PREFIX);
    assertThat(
            embeddedOptimizeExtension
                .getDatabaseSchemaManager()
                .schemaExists(prefixAwareDatabaseClient))
        .isTrue();
  }

  @Test
  public void allTypesWithPrefixExistAfterSchemaInitialization() throws IOException {
    // given
    embeddedOptimizeExtension
        .getConfigurationService()
        .getElasticSearchConfiguration()
        .setIndexPrefix(CUSTOM_PREFIX);
    embeddedOptimizeExtension.reloadConfiguration();

    // when
    initializeSchema();

    // then
    final List<IndexMappingCreator<?>> mappings =
        embeddedOptimizeExtension.getDatabaseSchemaManager().getMappings();
    assertThat(mappings).hasSize(29);
    for (IndexMappingCreator mapping : mappings) {
      final String expectedAliasName =
          getOptimizeIndexAliasForIndexNameAndPrefix(mapping.getIndexName(), CUSTOM_PREFIX);
      final String expectedIndexName =
          getOptimizeIndexOrTemplateNameForAliasAndVersion(
                  expectedAliasName, String.valueOf(mapping.getVersion()))
              + mapping.getIndexNameInitialSuffix();

      final OptimizeElasticsearchClient esClient =
          customPrefixDatabaseIntegrationTestExtension.getOptimizeElasticsearchClient();
      final RestHighLevelClient highLevelClient = esClient.getHighLevelClient();

      assertThat(
              highLevelClient
                  .indices()
                  .exists(new GetIndexRequest(expectedAliasName), esClient.requestOptions()))
          .isTrue();
      assertThat(
              highLevelClient
                  .indices()
                  .exists(new GetIndexRequest(expectedIndexName), esClient.requestOptions()))
          .isTrue();
    }
  }

  @Test
  public void optimizeIndexDataIsIsolated() {
    // given
    deploySimpleProcess();
    importAllEngineEntitiesFromScratch();

    String indexPrefix =
        customPrefixDatabaseIntegrationTestExtension.getIndexNameService().getIndexPrefix();

    // when
    // Set values for both ES and OS, the proper one will be used depending on which database is
    // active
    embeddedOptimizeExtension
        .getConfigurationService()
        .getElasticSearchConfiguration()
        .setIndexPrefix(indexPrefix);
    embeddedOptimizeExtension
        .getConfigurationService()
        .getOpenSearchConfiguration()
        .setIndexPrefix(indexPrefix);
    embeddedOptimizeExtension.reloadConfiguration();
    initializeSchema();

    deploySimpleProcess();

    importAllEngineEntitiesFromScratch();
    customPrefixDatabaseIntegrationTestExtension.refreshAllOptimizeIndices();

    // then
    assertThat(databaseIntegrationTestExtension.getDocumentCountOf(PROCESS_INSTANCE_MULTI_ALIAS))
        .isEqualTo(1);
    assertThat(
            customPrefixDatabaseIntegrationTestExtension.getDocumentCountOf(
                PROCESS_INSTANCE_MULTI_ALIAS))
        .isEqualTo(2);
  }

  private void deploySimpleProcess() {
    engineIntegrationExtension.deployAndStartProcess(createSimpleProcess());
  }

  private BpmnModelInstance createSimpleProcess() {
    return BpmnModels.getSingleServiceTaskProcess("aProcess");
  }

  private void initializeSchema() {
    embeddedOptimizeExtension
        .getDatabaseSchemaManager()
        .initializeSchema(prefixAwareDatabaseClient);
  }
}
