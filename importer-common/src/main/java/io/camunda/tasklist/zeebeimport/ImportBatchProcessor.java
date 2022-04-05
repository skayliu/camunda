/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */
package io.camunda.tasklist.zeebeimport;

import io.camunda.tasklist.exceptions.PersistenceException;

public interface ImportBatchProcessor {

  void performImport(ImportBatch importBatch) throws PersistenceException;

  String getZeebeVersion();
}
