package org.camunda.optimize.service.alert;

import org.camunda.optimize.dto.optimize.query.alert.AlertDefinitionDto;
import org.camunda.optimize.dto.optimize.query.alert.AlertInterval;
import org.springframework.stereotype.Component;


@Component
public class AlertCheckJobFactory extends AbstractAlertFactory<AlertJob> {

  protected String getTriggerGroup() {
    return "statusCheck-trigger";
  }

  protected String getTriggerName(AlertDefinitionDto alert) {
    return alert.getId() + "-check-trigger";
  }

  protected String getJobGroup() {
    return "statusCheck-job";
  }

  protected String getJobName(AlertDefinitionDto alert) {
    return alert.getId() + "-check-job";
  }

  @Override
  protected AlertInterval getInterval(AlertDefinitionDto alert) {
    return alert.getCheckInterval();
  }

  @Override
  protected Class<AlertJob> getJobClass() {
    return AlertJob.class;
  }
}
