/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */
package io.camunda.operate.zeebeimport;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.PostConstruct;

import io.camunda.operate.Metrics;
import io.camunda.operate.entities.meta.ImportPositionEntity;
import io.camunda.operate.schema.indices.ImportPositionIndex;
import io.camunda.operate.property.OperateProperties;
import io.camunda.operate.util.Either;
import io.camunda.operate.util.ElasticsearchUtil;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import static io.camunda.operate.util.ElasticsearchUtil.joinWithAnd;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Component
@DependsOn("schemaStartup")
public class ImportPositionHolder {

  private static final Logger logger = LoggerFactory.getLogger(ImportPositionHolder.class);

  //this is the in-memory only storage
  private Map<String, ImportPositionEntity> lastScheduledPositions = new HashMap<>();

  private Map<String, ImportPositionEntity> pendingImportPositionUpdates = new HashMap<>();
  private Map<String, ImportPositionEntity> inflightImportPositions = new HashMap<>();

  private ScheduledFuture<?> scheduledImportPositionUpdateTask;
  private ReentrantLock inflightImportPositionLock = new ReentrantLock();

  @Autowired
  private ImportPositionIndex importPositionType;

  @Autowired
  private RestHighLevelClient esClient;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private OperateProperties operateProperties;

  @Autowired
  private Metrics metrics;

  @Autowired
  @Qualifier("importPositionUpdateThreadPoolExecutor")
  private ThreadPoolTaskScheduler importPositionUpdateExecutor;

  @PostConstruct
  private void init() {
    logger.info("INIT: Start import position updater...");
    scheduleImportPositionUpdateTask();
  }

  public void scheduleImportPositionUpdateTask() {
    final var interval = operateProperties.getImporter().getImportPositionUpdateInterval();
    scheduledImportPositionUpdateTask = importPositionUpdateExecutor.schedule(
        this::updateImportPositions,
        OffsetDateTime.now().plus(interval, ChronoUnit.MILLIS).toInstant());
  }

  public CompletableFuture<Void> cancelScheduledImportPositionUpdateTask() {
    final var future = new CompletableFuture<Void>();
    importPositionUpdateExecutor.submit(() -> {
      if (scheduledImportPositionUpdateTask != null) {
        scheduledImportPositionUpdateTask.cancel(false);
        scheduledImportPositionUpdateTask = null;
      }

      future.complete(null);
    });
    return future;
  }

  public ImportPositionEntity getLatestScheduledPosition(String aliasTemplate, int partitionId) throws IOException {
    String key = getKey(aliasTemplate, partitionId);
    if (lastScheduledPositions.containsKey(key)) {
      return lastScheduledPositions.get(key);
    } else {
      ImportPositionEntity latestLoadedPosition = getLatestLoadedPosition(aliasTemplate, partitionId);
      lastScheduledPositions.put(key, latestLoadedPosition);
      return latestLoadedPosition;
    }
  }

  public void recordLatestScheduledPosition(String aliasName, int partitionId, ImportPositionEntity importPositionEntity) {
    lastScheduledPositions.put(getKey(aliasName, partitionId), importPositionEntity);
  }

  public ImportPositionEntity getLatestLoadedPosition(String aliasTemplate, int partitionId) throws IOException {
    final QueryBuilder queryBuilder = joinWithAnd(termQuery(ImportPositionIndex.ALIAS_NAME, aliasTemplate),
        termQuery(ImportPositionIndex.PARTITION_ID, partitionId));

    final SearchRequest searchRequest = new SearchRequest(importPositionType.getAlias())
        .source(new SearchSourceBuilder()
            .query(queryBuilder)
            .size(10));

    final SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);

    final Iterator<SearchHit> hitIterator = searchResponse.getHits().iterator();

    ImportPositionEntity position = new ImportPositionEntity(aliasTemplate, partitionId, 0);

    if (hitIterator.hasNext()) {
      position = ElasticsearchUtil.fromSearchHit(hitIterator.next().getSourceAsString(), objectMapper, ImportPositionEntity.class);
    }
    logger.debug("Latest loaded position for alias [{}] and partitionId [{}]: {}", aliasTemplate, partitionId, position);

    return position;
  }

  public void recordLatestLoadedPosition(ImportPositionEntity lastProcessedPosition) {
    withInflightImportPositionLock(() -> {
      final var aliasName = lastProcessedPosition.getAliasName();
      final var partition = lastProcessedPosition.getPartitionId();
      inflightImportPositions.put(getKey(aliasName, partition), lastProcessedPosition);
    });
  }

  public void updateImportPositions() {
    withInflightImportPositionLock(() -> {
      pendingImportPositionUpdates.putAll(inflightImportPositions);
      inflightImportPositions.clear();
    });

    final var result = updateImportPositions(pendingImportPositionUpdates);

    if (result.getOrElse(false)) {
      // clear only map when updating the import positions
      // succeeded, otherwise, it may result in lost updates
      pendingImportPositionUpdates.clear();
    }

    // self scheduling just for the case the interval is set too short
    scheduleImportPositionUpdateTask();
  }

  private Either<Throwable, Boolean> updateImportPositions(final Map<String, ImportPositionEntity> positions) {
    final var preparedBulkRequest = prepareBulkRequest(positions);

    if (preparedBulkRequest.isLeft()) {
      final var e = preparedBulkRequest.getLeft();
      return Either.left(e);
    }

    try {
      final var bulkRequest = preparedBulkRequest.get();

      withImportPositionTimer(() -> {
        ElasticsearchUtil.processBulkRequest(esClient, bulkRequest);
        return null;
      });

      return Either.right(true);
    } catch (final Throwable e) {
      logger.error(String.format("Error occurred while persisting latest loaded position", e));
      return Either.left(e);
    }
  }

  private Either<Exception, BulkRequest> prepareBulkRequest(final Map<String, ImportPositionEntity> positions) {
    final var bulkRequest = new BulkRequest();

    if (positions.size() > 0) {
      final var preparedUpdateRequests = positions.values()
          .stream()
          .map(this::prepareUpdateRequest)
          .collect(Either.collectorFoldingLeft());

      if (preparedUpdateRequests.isLeft()) {
        final var e = preparedUpdateRequests.getLeft();
        return Either.left(e);
      }

      preparedUpdateRequests.get().forEach(bulkRequest::add);
    }

    return Either.right(bulkRequest);
  }

  private Either<Exception, UpdateRequest> prepareUpdateRequest(final ImportPositionEntity position) {
    try {
      final var index = importPositionType.getFullQualifiedName();
      final var source = objectMapper.writeValueAsString(position);
      final var updateFields = new HashMap<String, Object>();

      updateFields.put(ImportPositionIndex.POSITION, position.getPosition());
      updateFields.put(ImportPositionIndex.FIELD_INDEX_NAME, position.getIndexName());

      final UpdateRequest updateRequest = new UpdateRequest()
          .index(index)
          .id(position.getId())
          .upsert(source, XContentType.JSON)
          .doc(updateFields);

      return Either.right(updateRequest);

    } catch (final Exception e) {
      logger.error(String.format("Error occurred while preparing request to update processed position for %s", position.getAliasName(), e));
      return Either.left(e);
    }
  }

  public void clearCache() {
    lastScheduledPositions.clear();
    pendingImportPositionUpdates.clear();

    withInflightImportPositionLock(() -> {
      inflightImportPositions.clear();
    });
  }

  private String getKey(String aliasTemplate, int partitionId) {
    return String.format("%s-%d", aliasTemplate, partitionId);
  }

  private void withImportPositionTimer(final Callable<Void> action) throws Exception {
    metrics.getTimer(Metrics.TIMER_NAME_IMPORT_POSITION_UPDATE).recordCallable(action);
  }

  private void withInflightImportPositionLock(final Runnable action) {
    try {
      inflightImportPositionLock.lock();
      action.run();
    } finally {
      inflightImportPositionLock.unlock();
    }
  }

}
