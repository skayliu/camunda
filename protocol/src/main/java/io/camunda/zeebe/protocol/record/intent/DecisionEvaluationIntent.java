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
package io.camunda.zeebe.protocol.record.intent;

public enum DecisionEvaluationIntent implements Intent {
  EVALUATED(0),
  FAILED(1),
  EVALUATE(2);

  private final short value;

  DecisionEvaluationIntent(final int value) {
    this.value = (short) value;
  }

  public short getIntent() {
    return value;
  }

  public static Intent from(final short value) {
    switch (value) {
      case 0:
        return EVALUATED;
      case 1:
        return FAILED;
      case 2:
        return EVALUATE;
      default:
        return UNKNOWN;
    }
  }

  @Override
  public short value() {
    return value;
  }
}
