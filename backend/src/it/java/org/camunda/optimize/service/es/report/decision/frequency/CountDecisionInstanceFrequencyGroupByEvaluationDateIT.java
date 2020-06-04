/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.service.es.report.decision.frequency;

import com.google.common.collect.Lists;
import org.camunda.optimize.dto.engine.definition.DecisionDefinitionEngineDto;
import org.camunda.optimize.dto.optimize.ReportConstants;
import org.camunda.optimize.dto.optimize.query.report.FilterOperatorConstants;
import org.camunda.optimize.dto.optimize.query.report.single.decision.DecisionReportDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.decision.filter.EvaluationDateFilterDto;
import org.camunda.optimize.dto.optimize.query.report.single.filter.data.date.DateFilterUnit;
import org.camunda.optimize.dto.optimize.query.report.single.filter.data.date.RollingDateFilterDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.filter.data.date.RollingDateFilterStartDto;
import org.camunda.optimize.dto.optimize.query.report.single.group.GroupByDateUnit;
import org.camunda.optimize.dto.optimize.query.report.single.result.ReportMapResultDto;
import org.camunda.optimize.dto.optimize.query.report.single.result.hyper.MapResultEntryDto;
import org.camunda.optimize.dto.optimize.query.sorting.SortOrder;
import org.camunda.optimize.dto.optimize.query.sorting.SortingDto;
import org.camunda.optimize.dto.optimize.rest.report.AuthorizedDecisionReportEvaluationResultDto;
import org.camunda.optimize.service.es.report.decision.AbstractDecisionDefinitionIT;
import org.camunda.optimize.test.util.decision.DecisionReportDataBuilder;
import org.camunda.optimize.test.util.decision.DecisionReportDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.ws.rs.core.Response;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.camunda.optimize.dto.optimize.query.sorting.SortingDto.SORT_BY_KEY;
import static org.camunda.optimize.dto.optimize.query.sorting.SortingDto.SORT_BY_VALUE;
import static org.camunda.optimize.test.util.decision.DecisionFilterUtilHelper.createNumericInputVariableFilter;
import static org.camunda.optimize.upgrade.es.ElasticsearchConstants.NUMBER_OF_DATA_POINTS_FOR_AUTOMATIC_INTERVAL_SELECTION;
import static org.camunda.optimize.util.DmnModels.INPUT_AMOUNT_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsNull.notNullValue;

public class CountDecisionInstanceFrequencyGroupByEvaluationDateIT extends AbstractDecisionDefinitionIT {

  @Test
  public void reportEvaluationSingleBucketSpecificVersionGroupedByDay() {
    // given
    DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    // different version
    DecisionDefinitionEngineDto decisionDefinitionDto2 = deployAndStartSimpleDecisionDefinition("key");
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto2.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final ReportMapResultDto result = evaluateDecisionInstanceFrequencyByEvaluationDate(
      decisionDefinitionDto1, decisionDefinitionVersion1, GroupByDateUnit.DAY
    ).getResult();

    // then
    assertThat(result.getIsComplete(), is(true));
    assertThat(result.getInstanceCount(), is(3L));
    assertThat(result.getData(), is(notNullValue()));
    assertThat(result.getData().size(), is(1));
    assertThat(result.getData().get(0).getValue(), is(3L));
  }

  @Test
  public void reportEvaluationMultiBucketsSpecificVersionGroupedByDay() throws SQLException {
    // given
    final OffsetDateTime beforeStart = OffsetDateTime.now();
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(beforeStart, beforeStart.minusDays(1));

    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final ReportMapResultDto result = evaluateDecisionInstanceFrequencyByEvaluationDate(
      decisionDefinitionDto1, decisionDefinitionVersion1, GroupByDateUnit.DAY
    ).getResult();

    // then
    assertThat(result.getInstanceCount(), is(5L));
    assertThat(result.getIsComplete(), is(true));
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData, is(notNullValue()));
    assertThat(resultData.size(), is(2));
    assertThat(resultData.get(0).getValue(), is(2L));
    assertThat(resultData.get(1).getValue(), is(3L));
  }

  @Test
  public void reportEvaluationMultiBucketsSpecificVersionGroupedByDayResultIsSortedInDescendingOrder()
    throws Exception {
    // given
    final OffsetDateTime beforeStart = OffsetDateTime.now();
    OffsetDateTime lastEvaluationDateFilter = beforeStart;

    // third bucket
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime thirdBucketEvaluationDate = beforeStart.minusDays(2);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, thirdBucketEvaluationDate);

    // second bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime secondBucketEvaluationDate = beforeStart.minusDays(1);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, secondBucketEvaluationDate);

    // first bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final ReportMapResultDto result = evaluateDecisionInstanceFrequencyByEvaluationDate(
      decisionDefinitionDto1, decisionDefinitionVersion1, GroupByDateUnit.DAY
    ).getResult();

    // then
    assertThat(result.getIsComplete(), is(true));
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData.size(), is(3));
    assertThat(
      resultData.get(0).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(lastEvaluationDateFilter, ChronoUnit.DAYS))
    );
    assertThat(resultData.get(0).getValue(), is(2L));
    assertThat(
      resultData.get(1).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(secondBucketEvaluationDate, ChronoUnit.DAYS))
    );
    assertThat(resultData.get(1).getValue(), is(2L));
    assertThat(
      resultData.get(2).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(thirdBucketEvaluationDate, ChronoUnit.DAYS))
    );
    assertThat(resultData.get(2).getValue(), is(3L));
  }

  @Test
  public void testCustomOrderOnResultKeyIsApplied() throws SQLException {
    // given
    final OffsetDateTime beforeStart = OffsetDateTime.now();
    OffsetDateTime lastEvaluationDateFilter = beforeStart;

    // third bucket
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime thirdBucketEvaluationDate = beforeStart.minusDays(2);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, thirdBucketEvaluationDate);

    // second bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime secondBucketEvaluationDate = beforeStart.minusDays(1);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, secondBucketEvaluationDate);

    // first bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey(decisionDefinitionDto1.getKey())
      .setDecisionDefinitionVersion(decisionDefinitionVersion1)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .setDateInterval(GroupByDateUnit.DAY)
      .build();
    reportData.getConfiguration().setSorting(new SortingDto(SORT_BY_KEY, SortOrder.ASC));
    final AuthorizedDecisionReportEvaluationResultDto<ReportMapResultDto> evaluationResult =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto result = evaluationResult.getResult();
    assertThat(result.getIsComplete(), is(true));
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData.size(), is(3));
    assertThat(
      resultData.get(0).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(thirdBucketEvaluationDate, ChronoUnit.DAYS))
    );
    assertThat(
      resultData.get(1).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(secondBucketEvaluationDate, ChronoUnit.DAYS))
    );
    assertThat(
      resultData.get(2).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(lastEvaluationDateFilter, ChronoUnit.DAYS))
    );
  }

  @Test
  public void testCustomOrderOnResultValueIsApplied() throws SQLException {
    // given
    final OffsetDateTime beforeStart = OffsetDateTime.now();
    OffsetDateTime lastEvaluationDateFilter = beforeStart;

    // third bucket
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime thirdBucketEvaluationDate = beforeStart.minusDays(2);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, thirdBucketEvaluationDate);

    // second bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime secondBucketEvaluationDate = beforeStart.minusDays(1);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, secondBucketEvaluationDate);

    // first bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey(decisionDefinitionDto1.getKey())
      .setDecisionDefinitionVersion(decisionDefinitionVersion1)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .setDateInterval(GroupByDateUnit.DAY)
      .build();
    reportData.getConfiguration().setSorting(new SortingDto(SORT_BY_VALUE, SortOrder.ASC));
    final AuthorizedDecisionReportEvaluationResultDto<ReportMapResultDto> evaluationResult =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto result = evaluationResult.getResult();
    assertThat(result.getIsComplete(), is(true));
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData.size(), is(3));
    final List<Long> bucketValues = resultData.stream().map(MapResultEntryDto::getValue).collect(Collectors.toList());
    assertThat(
      bucketValues,
      contains(bucketValues.stream().sorted(Comparator.naturalOrder()).toArray())
    );
  }

  @Test
  public void testEmptyBucketsAreReturnedForEvaluationDateFilterPeriod() throws SQLException {
    // given
    final OffsetDateTime startDate = OffsetDateTime.now();

    // third bucket
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime secondBucketEvaluationDate = startDate.minusDays(2);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(startDate, secondBucketEvaluationDate);

    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final RollingDateFilterDataDto dateFilterDataDto = new RollingDateFilterDataDto(
      new RollingDateFilterStartDto(4L, DateFilterUnit.DAYS)
    );

    final EvaluationDateFilterDto dateFilterDto = new EvaluationDateFilterDto(dateFilterDataDto);

    final DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey(decisionDefinitionDto1.getKey())
      .setDecisionDefinitionVersion(decisionDefinitionVersion1)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .setDateInterval(GroupByDateUnit.DAY)
      .setFilter(dateFilterDto)
      .build();

    final AuthorizedDecisionReportEvaluationResultDto<ReportMapResultDto> evaluationResult =
      reportClient.evaluateMapReport(reportData);

    // then
    final List<MapResultEntryDto> resultData = evaluationResult.getResult().getData();
    assertThat(resultData.size(), is(5));

    assertThat(
      resultData.get(0).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(startDate, ChronoUnit.DAYS))
    );
    assertThat(resultData.get(0).getValue(), is(1L));

    assertThat(
      resultData.get(1).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(startDate.minusDays(1), ChronoUnit.DAYS))
    );
    assertThat(resultData.get(1).getValue(), is(0L));

    assertThat(
      resultData.get(2).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(startDate.minusDays(2), ChronoUnit.DAYS))
    );
    assertThat(resultData.get(2).getValue(), is(2L));

    assertThat(
      resultData.get(3).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(startDate.minusDays(3), ChronoUnit.DAYS))
    );
    assertThat(resultData.get(3).getValue(), is(0L));

    assertThat(
      resultData.get(4).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(startDate.minusDays(4), ChronoUnit.DAYS))
    );
    assertThat(resultData.get(4).getValue(), is(0L));
  }

  @Test
  public void multipleBuckets_noFilter_resultLimitedByConfig() throws SQLException {
    // given
    final OffsetDateTime beforeStart = OffsetDateTime.now();
    OffsetDateTime lastEvaluationDateFilter = beforeStart;

    // third bucket
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime thirdBucketEvaluationDate = beforeStart.minusDays(2);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, thirdBucketEvaluationDate);

    // second bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime secondBucketEvaluationDate = beforeStart.minusDays(1);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, secondBucketEvaluationDate);

    // first bucket
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    embeddedOptimizeExtension.getConfigurationService().setEsAggregationBucketLimit(2);

    // when
    final DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey(decisionDefinitionDto1.getKey())
      .setDecisionDefinitionVersion(decisionDefinitionVersion1)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .setDateInterval(GroupByDateUnit.DAY)
      .build();
    final AuthorizedDecisionReportEvaluationResultDto<ReportMapResultDto> evaluationResult =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto result = evaluationResult.getResult();
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(result.getIsComplete(), is(false));
    assertThat(resultData.size(), is(2));
  }

  @ParameterizedTest
  @MethodSource("groupByDateUnits")
  public void reportEvaluationMultiBucketsSpecificVersionGroupedByDifferentUnitsEmptyBucketBetweenTwoOthers(
    final GroupByDateUnit groupByDateUnit
  ) throws Exception {
    // given
    final OffsetDateTime beforeStart = OffsetDateTime.now();
    final ChronoUnit chronoUnit = ChronoUnit.valueOf(groupByDateUnit.name().toUpperCase() + "S");
    OffsetDateTime lastEvaluationDateFilter = beforeStart;

    // third bucket
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime thirdBucketEvaluationDate = beforeStart.minus(2, chronoUnit);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(lastEvaluationDateFilter, thirdBucketEvaluationDate);

    // second empty bucket
    final OffsetDateTime secondBucketEvaluationDate = beforeStart.minus(1, chronoUnit);

    // first bucket
    lastEvaluationDateFilter = OffsetDateTime.now();
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final ReportMapResultDto result = evaluateDecisionInstanceFrequencyByEvaluationDate(
      decisionDefinitionDto1, decisionDefinitionVersion1, groupByDateUnit
    ).getResult();

    // then
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData.size(), is(3));
    assertThat(
      resultData.get(0).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(lastEvaluationDateFilter, chronoUnit))
    );
    assertThat(resultData.get(0).getValue(), is(2L));
    assertThat(
      resultData.get(1).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(secondBucketEvaluationDate, chronoUnit))
    );
    assertThat(resultData.get(1).getValue(), is(0L));
    assertThat(
      resultData.get(2).getKey(),
      is(embeddedOptimizeExtension.formatToHistogramBucketKey(thirdBucketEvaluationDate, chronoUnit))
    );
    assertThat(resultData.get(2).getValue(), is(3L));
  }

  @Test
  public void automaticIntervalSelectionWorks() throws Exception {
    // given
    final OffsetDateTime beforeStart = OffsetDateTime.now();

    // third bucket
    final DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    final String decisionDefinitionVersion1 = String.valueOf(decisionDefinitionDto1.getVersion());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    final OffsetDateTime thirdBucketEvaluationDate = beforeStart.minus(5, ChronoUnit.DAYS);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(beforeStart, thirdBucketEvaluationDate);

    // second empty bucket

    // first bucket
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    final OffsetDateTime firstBucketEvaluationDate = beforeStart.minus(1, ChronoUnit.DAYS);
    engineDatabaseExtension.changeDecisionInstanceEvaluationDate(beforeStart, firstBucketEvaluationDate);

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final ReportMapResultDto result = evaluateDecisionInstanceFrequencyByEvaluationDate(
      decisionDefinitionDto1, decisionDefinitionVersion1, GroupByDateUnit.AUTOMATIC
    ).getResult();

    // then
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData.size(), is(NUMBER_OF_DATA_POINTS_FOR_AUTOMATIC_INTERVAL_SELECTION));
    assertThat(resultData.get(0).getValue(), is(2L));
    assertThat(resultData.stream().map(MapResultEntryDto::getValue).mapToInt(Long::intValue).sum(), is(5));
    assertThat(resultData.get(resultData.size() - 1).getValue(), is(3L));
  }

  @Test
  public void reportEvaluationSingleBucketAllVersionsGroupByYear() {
    // given
    DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    // different version
    DecisionDefinitionEngineDto decisionDefinitionDto2 = deployAndStartSimpleDecisionDefinition("key");
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto2.getId());

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final ReportMapResultDto result = evaluateDecisionInstanceFrequencyByEvaluationDate(
      decisionDefinitionDto1, ReportConstants.ALL_VERSIONS, GroupByDateUnit.YEAR
    ).getResult();

    // then
    assertThat(result.getInstanceCount(), is(5L));
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData, is(notNullValue()));
    assertThat(resultData.size(), is(1));
    assertThat(resultData.get(0).getValue(), is(5L));
  }

  @Test
  public void reportEvaluationSingleBucketAllVersionsGroupByYearOtherDefinitionsHaveNoSideEffect() {
    // given
    DecisionDefinitionEngineDto decisionDefinitionDto1 = deployAndStartSimpleDecisionDefinition("key");
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto1.getId());

    // different version
    DecisionDefinitionEngineDto decisionDefinitionDto2 = deployAndStartSimpleDecisionDefinition("key");
    engineIntegrationExtension.startDecisionInstance(decisionDefinitionDto2.getId());

    // other decision definition
    deployAndStartSimpleDecisionDefinition("key2");

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    final ReportMapResultDto result = evaluateDecisionInstanceFrequencyByEvaluationDate(
      decisionDefinitionDto1, ReportConstants.ALL_VERSIONS, GroupByDateUnit.YEAR
    ).getResult();

    // then
    assertThat(result.getInstanceCount(), is(5L));
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData, is(notNullValue()));
    assertThat(resultData.size(), is(1));
    assertThat(resultData.get(0).getValue(), is(5L));
  }

  @Test
  public void reportEvaluationSingleBucketFilteredBySingleTenant() {
    // given
    final String tenantId1 = "tenantId1";
    final String tenantId2 = "tenantId2";
    final List<String> selectedTenants = Lists.newArrayList(tenantId1);
    final String decisionDefinitionKey = deployAndStartMultiTenantDefinition(
      Lists.newArrayList(null, tenantId1, tenantId2)
    );

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey(decisionDefinitionKey)
      .setDecisionDefinitionVersion(ReportConstants.ALL_VERSIONS)
      .setTenantIds(selectedTenants)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .setDateInterval(GroupByDateUnit.HOUR)
      .build();
    ReportMapResultDto result = reportClient.evaluateMapReport(reportData).getResult();

    // then
    assertThat(result.getInstanceCount(), is((long) selectedTenants.size()));
  }

  @Test
  public void reportEvaluationSingleBucketFilteredByInputValue() {
    // given
    final double inputVariableValueToFilterFor = 200.0;
    final DecisionDefinitionEngineDto decisionDefinitionDto = engineIntegrationExtension.deployDecisionDefinition();
    startDecisionInstanceWithInputVars(
      decisionDefinitionDto.getId(),
      createInputs(100.0, "Misc")
    );
    startDecisionInstanceWithInputVars(
      decisionDefinitionDto.getId(),
      createInputs(inputVariableValueToFilterFor, "Misc")
    );
    startDecisionInstanceWithInputVars(
      decisionDefinitionDto.getId(),
      createInputs(inputVariableValueToFilterFor + 100.0, "Misc")
    );

    embeddedOptimizeExtension.importAllEngineEntitiesFromScratch();
    elasticSearchIntegrationTestExtension.refreshAllOptimizeIndices();

    // when
    DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey(decisionDefinitionDto.getKey())
      .setDecisionDefinitionVersion(String.valueOf(decisionDefinitionDto.getVersion()))
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .setDateInterval(GroupByDateUnit.HOUR)
      .setFilter(createNumericInputVariableFilter(
        INPUT_AMOUNT_ID, FilterOperatorConstants.GREATER_THAN_EQUALS, String.valueOf(inputVariableValueToFilterFor)
      ))
      .build();
    final AuthorizedDecisionReportEvaluationResultDto<ReportMapResultDto> evaluationResult =
      reportClient.evaluateMapReport(reportData);

    // then
    final ReportMapResultDto result = evaluationResult.getResult();
    assertThat(result.getInstanceCount(), is(2L));
    final List<MapResultEntryDto> resultData = result.getData();
    assertThat(resultData, is(notNullValue()));
    assertThat(resultData.size(), is(1));
    assertThat(resultData.get(0).getValue(), is(2L));
  }

  @Test
  public void optimizeExceptionOnViewPropertyIsNull() {
    // given
    DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey("key")
      .setDecisionDefinitionVersion(ReportConstants.ALL_VERSIONS)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .build();
    reportData.getView().setProperty(null);

    //when
    Response response = reportClient.evaluateReportAndReturnResponse(reportData);

    // then
    assertThat(response.getStatus(), is(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()));
  }

  private static GroupByDateUnit[] groupByDateUnits() {
    return Arrays.stream(GroupByDateUnit.values())
      .filter(v -> !v.equals(GroupByDateUnit.AUTOMATIC))
      .toArray(GroupByDateUnit[]::new);
  }

  @Test
  public void optimizeExceptionOnGroupByTypeIsNull() {
    // given
    DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey("key")
      .setDecisionDefinitionVersion(ReportConstants.ALL_VERSIONS)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .build();
    reportData.getGroupBy().setType(null);

    //when
    Response response = reportClient.evaluateReportAndReturnResponse(reportData);

    // then
    assertThat(response.getStatus(), is(Response.Status.BAD_REQUEST.getStatusCode()));
  }

  private AuthorizedDecisionReportEvaluationResultDto<ReportMapResultDto> evaluateDecisionInstanceFrequencyByEvaluationDate(
    final DecisionDefinitionEngineDto decisionDefinitionDto,
    final String decisionDefinitionVersion,
    final GroupByDateUnit groupByDateUnit) {
    DecisionReportDataDto reportData = DecisionReportDataBuilder.create()
      .setDecisionDefinitionKey(decisionDefinitionDto.getKey())
      .setDecisionDefinitionVersion(decisionDefinitionVersion)
      .setReportDataType(DecisionReportDataType.COUNT_DEC_INST_FREQ_GROUP_BY_EVALUATION_DATE_TIME)
      .setDateInterval(groupByDateUnit)
      .build();
    return reportClient.evaluateMapReport(reportData);
  }

}
