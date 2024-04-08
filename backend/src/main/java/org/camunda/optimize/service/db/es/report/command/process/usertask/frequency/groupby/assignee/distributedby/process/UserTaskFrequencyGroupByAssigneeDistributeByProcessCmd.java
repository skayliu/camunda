/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.service.db.es.report.command.process.usertask.frequency.groupby.assignee.distributedby.process;

import java.util.List;
import org.camunda.optimize.dto.optimize.query.report.single.result.hyper.HyperMapResultEntryDto;
import org.camunda.optimize.service.db.es.report.command.ProcessCmd;
import org.camunda.optimize.service.db.es.report.command.exec.ProcessReportCmdExecutionPlan;
import org.camunda.optimize.service.db.es.report.command.exec.builder.ReportCmdExecutionPlanBuilder;
import org.camunda.optimize.service.db.es.report.command.modules.distributed_by.process.ProcessDistributedByProcess;
import org.camunda.optimize.service.db.es.report.command.modules.group_by.process.identity.ProcessGroupByAssignee;
import org.camunda.optimize.service.db.es.report.command.modules.view.process.frequency.ProcessViewUserTaskFrequency;
import org.springframework.stereotype.Component;

@Component
public class UserTaskFrequencyGroupByAssigneeDistributeByProcessCmd
    extends ProcessCmd<List<HyperMapResultEntryDto>> {

  public UserTaskFrequencyGroupByAssigneeDistributeByProcessCmd(
      final ReportCmdExecutionPlanBuilder builder) {
    super(builder);
  }

  @Override
  protected ProcessReportCmdExecutionPlan<List<HyperMapResultEntryDto>> buildExecutionPlan(
      final ReportCmdExecutionPlanBuilder builder) {
    return builder
        .createExecutionPlan()
        .processCommand()
        .view(ProcessViewUserTaskFrequency.class)
        .groupBy(ProcessGroupByAssignee.class)
        .distributedBy(ProcessDistributedByProcess.class)
        .resultAsHyperMap()
        .build();
  }

  @Override
  public boolean isAssigneeReport() {
    return true;
  }
}
