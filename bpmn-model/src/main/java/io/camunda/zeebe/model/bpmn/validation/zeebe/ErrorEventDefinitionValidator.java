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
package io.camunda.zeebe.model.bpmn.validation.zeebe;

import io.camunda.zeebe.model.bpmn.instance.EndEvent;
import io.camunda.zeebe.model.bpmn.instance.Error;
import io.camunda.zeebe.model.bpmn.instance.ErrorEventDefinition;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.camunda.bpm.model.xml.validation.ModelElementValidator;
import org.camunda.bpm.model.xml.validation.ValidationResultCollector;

public class ErrorEventDefinitionValidator implements ModelElementValidator<ErrorEventDefinition> {

  private static final String ZEEBE_EXPRESSION_PREFIX = "=";

  @Override
  public Class<ErrorEventDefinition> getElementType() {
    return ErrorEventDefinition.class;
  }

  @Override
  public void validate(
      final ErrorEventDefinition element,
      final ValidationResultCollector validationResultCollector) {

    final ModelElementInstance parentElement = element.getParentElement();
    final Error error = element.getError();
    if (parentElement instanceof EndEvent) {
      if (error == null) {
        validationResultCollector.addError(0, "Must reference an error");
      } else {
        final String errorCode = error.getErrorCode();
        if (errorCode == null || errorCode.isEmpty()) {
          validationResultCollector.addError(0, "ErrorCode must be present and not empty");
        }
      }
    } else {
      if (error != null) {
        final String errorCode = error.getErrorCode();
        if (errorCode != null && errorCode.startsWith(ZEEBE_EXPRESSION_PREFIX)) {
          validationResultCollector.addError(
              0, "The errorCode of the error catch event is not allowed to be an expression");
        }
      }
    }
  }
}
