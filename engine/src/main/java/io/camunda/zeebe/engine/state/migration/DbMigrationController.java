/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.migration;

import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.stream.api.ReadonlyStreamProcessorContext;
import io.camunda.zeebe.stream.api.StreamProcessorLifecycleAware;
import java.util.function.Function;

public final class DbMigrationController implements StreamProcessorLifecycleAware {

  private DbMigrator dbMigrator;
  private final Function<MutableZeebeState, DbMigrator> migratorFactory;
  private final MutableZeebeState mutableZeebeState;

  public DbMigrationController(final MutableZeebeState mutableZeebeState) {
    this(mutableZeebeState, DbMigratorImpl::new);
  }

  DbMigrationController(
      final MutableZeebeState mutableZeebeState,
      final Function<MutableZeebeState, DbMigrator> migratorFactory) {
    this.mutableZeebeState = mutableZeebeState;
    this.migratorFactory = migratorFactory;
  }

  @Override
  public final void onRecovered(final ReadonlyStreamProcessorContext context) {
    final var migrator = migratorFactory.apply(mutableZeebeState);

    synchronized (this) {
      dbMigrator = migrator;
    }
    try {
      migrator.runMigrations();
    } finally {
      synchronized (this) {
        dbMigrator = null;
      }
    }
  }

  @Override
  public final void onClose() {
    abortMigrationIfRunning();
  }

  @Override
  public final void onFailed() {
    abortMigrationIfRunning();
  }

  private void abortMigrationIfRunning() {
    synchronized (this) {
      if (dbMigrator != null) {
        dbMigrator.abort();
      }
    }
  }
}
