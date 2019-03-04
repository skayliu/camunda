/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

const getMaxPage = (total, perPage) => Math.ceil(total / perPage);

const isAnyInstanceSelected = selection => {
  const {all, ids} = selection;
  return all || ids.length > 0;
};

export {getMaxPage, isAnyInstanceSelected};
