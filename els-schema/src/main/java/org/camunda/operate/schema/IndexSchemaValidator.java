package org.camunda.operate.schema;

import org.camunda.operate.es.RetryElasticsearchClient;
import org.camunda.operate.exceptions.OperateRuntimeException;
import org.camunda.operate.property.OperateProperties;
import org.camunda.operate.schema.indices.IndexDescriptor;
import org.camunda.operate.schema.migration.SemanticVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.camunda.operate.util.CollectionUtil.map;

@Component
public class IndexSchemaValidator {

  private static final Logger logger = LoggerFactory.getLogger(IndexSchemaValidator.class);

  private static final Pattern VERSION_PATTERN = Pattern.compile(".*-(\\d+\\.\\d+\\.\\d+.*)_.*");

  @Autowired
  Set<IndexDescriptor> indexDescriptors;

  @Autowired
  OperateProperties operateProperties;

  @Autowired
  RetryElasticsearchClient retryElasticsearchClient;

  private List<String> getAllIndexNamesForIndex(String index) {
    final String indexPattern = String.format("%s-%s*", getIndexPrefix(), index);
    logger.debug("Getting all indices for {}", indexPattern);
    return retryElasticsearchClient.getIndexNamesFor(indexPattern);
  }

  private String getIndexPrefix() {
    return operateProperties.getElasticsearch().getIndexPrefix();
  }

  public Set<String> newerVersionsForIndex(IndexDescriptor indexDescriptor) {
    SemanticVersion currentVersion = SemanticVersion.fromVersion(indexDescriptor.getVersion());
    Set<String> versions = versionsForIndex(indexDescriptor);
    return versions.stream()
        .filter(version -> SemanticVersion.fromVersion(version).isNewerThan(currentVersion))
        .collect(Collectors.toSet());
  }

  public Set<String> olderVersionsForIndex(IndexDescriptor indexDescriptor) {
    SemanticVersion currentVersion = SemanticVersion.fromVersion(indexDescriptor.getVersion());
    Set<String> versions = versionsForIndex(indexDescriptor);
    return versions.stream()
        .filter(version -> currentVersion.isNewerThan(SemanticVersion.fromVersion(version)))
        .collect(Collectors.toSet());
  }

  private Set<String> versionsForIndex(IndexDescriptor indexDescriptor) {
    List<String> allIndexNames = getAllIndexNamesForIndex(indexDescriptor.getIndexName());
    return allIndexNames.stream()
        .map(this::getVersionFromIndexName)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());
  }

  private Optional<String> getVersionFromIndexName(String indexName) {
    Matcher matcher = VERSION_PATTERN.matcher(indexName);
    if (matcher.matches() && matcher.groupCount() > 0) {
      return Optional.of(matcher.group(1));
    }
    return Optional.empty();
  }

  public void validate() {
    if (!hasAnyOperateIndices())
      return;
    Set<String> errors = new HashSet<>();
    indexDescriptors.forEach(indexDescriptor -> {
      Set<String> oldVersions = olderVersionsForIndex(indexDescriptor);
      Set<String> newerVersions = newerVersionsForIndex(indexDescriptor);
      if (oldVersions.size() > 1) {
        errors
            .add(String.format("More than one older version for %s (%s) found: %s", indexDescriptor.getIndexName(), indexDescriptor.getVersion(), oldVersions));
      }
      if (!newerVersions.isEmpty()) {
        errors
            .add(String.format("Newer version(s) for %s (%s) already exists: %s", indexDescriptor.getIndexName(), indexDescriptor.getVersion(), newerVersions));
      }
    });
    if (!errors.isEmpty()) {
      throw new OperateRuntimeException("Error(s) in index schema: " + String.join(";", errors));
    }
  }

  public boolean hasAnyOperateIndices() {
    final Set<String> indices = retryElasticsearchClient
        .getIndexNamesFromClusterHealth(operateProperties.getElasticsearch().getIndexPrefix() + "*");
    return !indices.isEmpty();
  }

  public boolean schemaExists() {
    try {
      final Set<String> indices = retryElasticsearchClient
          .getIndexNamesFromClusterHealth(operateProperties.getElasticsearch().getIndexPrefix() + "*");
      List<String> allIndexNames = map(indexDescriptors, IndexDescriptor::getFullQualifiedName);
      return indices.containsAll(allIndexNames);
    } catch (Exception e) {
      logger.error("Check for existing schema failed", e);
      return false;
    }
  }
}
