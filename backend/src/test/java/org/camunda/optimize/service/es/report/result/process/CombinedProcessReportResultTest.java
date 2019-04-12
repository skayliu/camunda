/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.service.es.report.result.process;

import com.google.common.collect.Lists;
import org.camunda.optimize.dto.optimize.query.report.combined.CombinedProcessReportResultDto;
import org.camunda.optimize.dto.optimize.query.report.combined.CombinedReportDefinitionDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.ProcessReportDataDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.SingleProcessReportDefinitionDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.ProcessReportMapResultDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.ProcessReportNumberResultDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.ProcessReportResultDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.duration.AggregationResultDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.duration.ProcessDurationReportMapResultDto;
import org.camunda.optimize.dto.optimize.query.report.single.process.result.duration.ProcessDurationReportNumberResultDto;
import org.camunda.optimize.dto.optimize.query.variable.VariableType;
import org.camunda.optimize.service.es.report.result.ReportEvaluationResult;
import org.camunda.optimize.test.util.ProcessReportDataBuilder;
import org.camunda.optimize.test.util.ProcessReportDataType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

public class CombinedProcessReportResultTest {

  @Test
  public void testGetResultAsCsvForMapResult() {
    // given
    final ProcessReportMapResultDto mapResultDto = new ProcessReportMapResultDto();
    final HashMap<String, Long> resultDtoMap = new HashMap<>();
    resultDtoMap.put("900.0", 1L);
    resultDtoMap.put("10.99", 1L);
    mapResultDto.setData(resultDtoMap);
    final List<ProcessReportResultDto> mapResultDtos = Lists.newArrayList(
      mapResultDto,
      mapResultDto
    );

    CombinedProcessReportResult underTest = createTestCombinedProcessReportResult(
      ProcessReportDataType.COUNT_PROC_INST_FREQ_GROUP_BY_VARIABLE,
      mapResultDtos
    );


    // when
    List<String[]> resultAsCsv = underTest.getResultAsCsv(10, 0, null);


    // then
    assertArrayEquals(new String[]{"SingleTestReport0", "", "", "SingleTestReport1", ""}, resultAsCsv.get(0));
    assertArrayEquals(
      new String[]{"variable_test_DOUBLE", "processInstance_frequency", "", "variable_test_DOUBLE",
        "processInstance_frequency"},
      resultAsCsv.get(1)
    );
    assertArrayEquals(new String[]{"900.0", "1", "", "900.0", "1"}, resultAsCsv.get(2));
    assertArrayEquals(new String[]{"10.99", "1", "", "10.99", "1"}, resultAsCsv.get(3));


    // when (limit = 0)
    resultAsCsv = underTest.getResultAsCsv(0, 0, null);


    // then
    assertArrayEquals(new String[]{"SingleTestReport0", "", "", "SingleTestReport1", ""}, resultAsCsv.get(0));
    assertArrayEquals(
      new String[]{"variable_test_DOUBLE", "processInstance_frequency", "", "variable_test_DOUBLE",
        "processInstance_frequency"},
      resultAsCsv.get(1)
    );
    assertArrayEquals(new String[]{"900.0", "1", "", "900.0", "1"}, resultAsCsv.get(2));


    // when (offset = 1)
    resultAsCsv = underTest.getResultAsCsv(0, 1, null);


    // then
    assertArrayEquals(new String[]{"SingleTestReport0", "", "", "SingleTestReport1", ""}, resultAsCsv.get(0));
    assertArrayEquals(
      new String[]{"variable_test_DOUBLE", "processInstance_frequency", "", "variable_test_DOUBLE",
        "processInstance_frequency"},
      resultAsCsv.get(1)
    );
    assertArrayEquals(new String[]{"10.99", "1", "", "10.99", "1"}, resultAsCsv.get(2));
  }

  @Test
  public void testGetResultAsCsvForNumberResult() {

    // given
    final ProcessReportNumberResultDto numberResultDto1 = new ProcessReportNumberResultDto();
    numberResultDto1.setData(5l);

    final ProcessReportNumberResultDto numberResultDto2 = new ProcessReportNumberResultDto();
    numberResultDto2.setData(2l);

    final ArrayList<ProcessReportResultDto> resultDtos = Lists.newArrayList(
      numberResultDto1,
      numberResultDto2
    );

    CombinedProcessReportResult underTest = createTestCombinedProcessReportResult(
      ProcessReportDataType.COUNT_PROC_INST_FREQ_GROUP_BY_NONE,
      resultDtos
    );

    // when
    final List<String[]> resultAsCsv = underTest.getResultAsCsv(10, 0, null);


    // then
    assertArrayEquals(
      new String[]{"SingleTestReport0", "", "SingleTestReport1"}, resultAsCsv.get(0)
    );
    assertArrayEquals(
      new String[]{"processInstance_frequency", "", "processInstance_frequency"},
      resultAsCsv.get(1)
    );
    assertArrayEquals(
      new String[]{"5", "", "2"}, resultAsCsv.get(2)
    );
  }

  @Test
  public void testGetResultAsCsvForDurationNumberResult() {

    // given

    final ProcessDurationReportNumberResultDto durReportDto = new ProcessDurationReportNumberResultDto();
    final AggregationResultDto aggroResult = new AggregationResultDto();
    aggroResult.setAvg(3l);
    aggroResult.setMedian(3l);
    aggroResult.setMin(1l);
    aggroResult.setMax(6l);
    durReportDto.setData(aggroResult);

    final ArrayList<ProcessReportResultDto> resultDtos = Lists.newArrayList(
      durReportDto,
      durReportDto
    );

    CombinedProcessReportResult underTest = createTestCombinedProcessReportResult(
      ProcessReportDataType.PROC_INST_DUR_GROUP_BY_NONE,
      resultDtos
    );


    // when
    final List<String[]> resultAsCsv = underTest.getResultAsCsv(10, 0, null);


    // then
    assertArrayEquals(
      new String[]{"SingleTestReport0", "", "", "", "", "SingleTestReport1", "", "", ""},
      resultAsCsv.get(0)
    );
    assertArrayEquals(
      new String[]{"processInstance_duration", "", "", "", "", "processInstance_duration", "", "", ""},
      resultAsCsv.get(1)
    );
    assertArrayEquals(
      new String[]{"minimum", "maximum", "average", "median", "", "minimum", "maximum", "average", "median"},
      resultAsCsv.get(2)
    );

    assertArrayEquals(new String[]{"1", "6", "3", "3", "", "1", "6", "3", "3"}, resultAsCsv.get(3));
  }


  @Test
  public void testGetResultAsCsvForDurationMapResult() {

    // given

    final ProcessDurationReportMapResultDto durMapReportDto = new ProcessDurationReportMapResultDto();
    final AggregationResultDto aggroResult = new AggregationResultDto();
    aggroResult.setAvg(3l);
    aggroResult.setMedian(3l);
    aggroResult.setMin(1l);
    aggroResult.setMax(6l);

    Map<String, AggregationResultDto> data = new HashMap<>();
    data.put("test1", aggroResult);
    data.put("test2", aggroResult);
    durMapReportDto.setData(data);

    final ArrayList<ProcessReportResultDto> resultDtos = Lists.newArrayList(
      durMapReportDto,
      durMapReportDto
    );

    CombinedProcessReportResult underTest = createTestCombinedProcessReportResult(
      ProcessReportDataType.PROC_INST_DUR_GROUP_BY_VARIABLE,
      resultDtos
    );


    // when
    List<String[]> resultAsCsv = underTest.getResultAsCsv(10, 0, null);


    // then
    assertArrayEquals(
      new String[]{"SingleTestReport0", "", "", "", "", "", "SingleTestReport1", "", "", "", ""},
      resultAsCsv.get(0)
    );
    assertArrayEquals(
      new String[]{"variable_test_DOUBLE", "processInstance_duration", "", "", "", "", "variable_test_DOUBLE",
        "processInstance_duration", "", "", ""},
      resultAsCsv.get(1)
    );
    assertArrayEquals(
      new String[]{"", "minimum", "maximum", "average", "median", "", "", "minimum", "maximum", "average", "median"},
      resultAsCsv.get(2)
    );
    assertArrayEquals(new String[]{"test2", "1", "6", "3", "3", "", "test2", "1", "6", "3", "3"}, resultAsCsv.get(3));
    assertArrayEquals(new String[]{"test1", "1", "6", "3", "3", "", "test1", "1", "6", "3", "3"}, resultAsCsv.get(4));

    // when (limit = 0)
    resultAsCsv = underTest.getResultAsCsv(0, 0, null);


    //then
    assertArrayEquals(
      new String[]{"SingleTestReport0", "", "", "", "", "", "SingleTestReport1", "", "", "", ""},
      resultAsCsv.get(0)
    );
    assertArrayEquals(
      new String[]{"variable_test_DOUBLE", "processInstance_duration", "", "", "", "", "variable_test_DOUBLE",
        "processInstance_duration", "", "", ""},
      resultAsCsv.get(1)
    );
    assertArrayEquals(
      new String[]{"", "minimum", "maximum", "average", "median", "", "", "minimum", "maximum", "average", "median"},
      resultAsCsv.get(2)
    );
    assertArrayEquals(new String[]{"test2", "1", "6", "3", "3", "", "test2", "1", "6", "3", "3"}, resultAsCsv.get(3));


    // when (offset = 1)
    resultAsCsv = underTest.getResultAsCsv(0, 1, null);


    //then
    assertArrayEquals(
      new String[]{"SingleTestReport0", "", "", "", "", "", "SingleTestReport1", "", "", "", ""},
      resultAsCsv.get(0)
    );
    assertArrayEquals(
      new String[]{"variable_test_DOUBLE", "processInstance_duration", "", "", "", "", "variable_test_DOUBLE",
        "processInstance_duration", "", "", ""},
      resultAsCsv.get(1)
    );
    assertArrayEquals(
      new String[]{"", "minimum", "maximum", "average", "median", "", "", "minimum", "maximum", "average", "median"},
      resultAsCsv.get(2)
    );
    assertArrayEquals(new String[]{"test1", "1", "6", "3", "3", "", "test1", "1", "6", "3", "3"}, resultAsCsv.get(3));
  }

  @Test
  public void testGetResultAsCsvForEmptyReport() {
    // given
    CombinedProcessReportResult underTest = new CombinedProcessReportResult(
      new CombinedProcessReportResultDto(new HashMap<String, ReportEvaluationResult<ProcessReportResultDto,
        SingleProcessReportDefinitionDto>>()),
      new CombinedReportDefinitionDto()
    );

    // when
    List<String[]> resultAsCsv = underTest.getResultAsCsv(10, 0, null);

    // then
    assertArrayEquals(new String[]{}, resultAsCsv.get(0));
  }


  private CombinedProcessReportResult createTestCombinedProcessReportResult(ProcessReportDataType reportDataType,
                                                                            List<ProcessReportResultDto> reportResultDtos) {

    final ProcessReportDataDto processReportDataDto = ProcessReportDataBuilder.createReportData()
      .setVariableName("test")
      .setReportDataType(reportDataType)
      .setVariableType(VariableType.DOUBLE)
      .build();

    List<ReportEvaluationResult> reportEvaluationResults = new ArrayList<>();

    for (int i = 0; i < reportResultDtos.size(); i++) {
      final SingleProcessReportDefinitionDto singleDefDto = new SingleProcessReportDefinitionDto();
      singleDefDto.setName("SingleTestReport" + i);
      singleDefDto.setData(processReportDataDto);

      reportEvaluationResults.add(createReportEvaluationResult(reportResultDtos.get(i), singleDefDto));
    }

    return createCombinedProcessReportResult(reportEvaluationResults);
  }

  private ReportEvaluationResult createReportEvaluationResult(final ProcessReportResultDto reportResultDto,
                                                              final SingleProcessReportDefinitionDto singleDefDto) {
    ReportEvaluationResult reportResult = null;

    if (reportResultDto instanceof ProcessReportMapResultDto) {
      reportResult = new SingleProcessMapReportResult((ProcessReportMapResultDto) reportResultDto, singleDefDto);

    } else if (reportResultDto instanceof ProcessReportNumberResultDto) {
      reportResult = new SingleProcessNumberReportResult((ProcessReportNumberResultDto) reportResultDto, singleDefDto);

    } else if (reportResultDto instanceof ProcessDurationReportNumberResultDto) {
      reportResult = new SingleProcessNumberDurationReportResult(
        (ProcessDurationReportNumberResultDto) reportResultDto,
        singleDefDto
      );
    } else if (reportResultDto instanceof ProcessDurationReportMapResultDto) {
      reportResult = new SingleProcessMapDurationReportResult(
        (ProcessDurationReportMapResultDto) reportResultDto,
        singleDefDto
      );
    }
    return reportResult;
  }

  private CombinedProcessReportResult createCombinedProcessReportResult(final List<ReportEvaluationResult> singleReportResults) {
    final LinkedHashMap<String, ReportEvaluationResult> mapIMap = new LinkedHashMap<>();

    for (int i = 0; i < singleReportResults.size(); i++) {
      mapIMap.put("test-id-" + i, singleReportResults.get(i));
    }

    return new CombinedProcessReportResult(
      new CombinedProcessReportResultDto(mapIMap),
      new CombinedReportDefinitionDto()
    );
  }


}