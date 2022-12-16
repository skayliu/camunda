/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.exporter.stream;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbString;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;

public final class ExportersState {

  public static final long VALUE_NOT_FOUND = -1;

  private static final UnsafeBuffer METADATA_NOT_FOUND = new UnsafeBuffer();

  private final DbString exporterId;
  private final ColumnFamily<DbString, ExporterStateEntry> exporterPositionColumnFamily;

  public ExportersState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    exporterId = new DbString();
    exporterPositionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.EXPORTER, transactionContext, exporterId, new ExporterStateEntry());
  }

  public void setPosition(final String exporterId, final long position) {
    setExporterState(exporterId, position, null);
  }

  public void setExporterState(
      final String exporterId, final long position, final DirectBuffer metadata) {
    this.exporterId.wrapString(exporterId);

    final var exporterStateEntry =
        findExporterStateEntry(exporterId).orElse(new ExporterStateEntry());
    exporterStateEntry.setPosition(position);
    if (metadata != null) {
      exporterStateEntry.setMetadata(metadata);
    }
    exporterPositionColumnFamily.upsert(this.exporterId, exporterStateEntry);
  }

  public long getPosition(final String exporterId) {
    return findExporterStateEntry(exporterId)
        .map(ExporterStateEntry::getPosition)
        .orElse(VALUE_NOT_FOUND);
  }

  public DirectBuffer getExporterMetadata(final String exporterId) {
    return findExporterStateEntry(exporterId)
        .map(ExporterStateEntry::getMetadata)
        .orElse(METADATA_NOT_FOUND);
  }

  private Optional<ExporterStateEntry> findExporterStateEntry(final String exporterId) {
    this.exporterId.wrapString(exporterId);
    return Optional.ofNullable(exporterPositionColumnFamily.get(this.exporterId));
  }

  public void visitExporterState(final BiConsumer<String, ExporterStateEntry> consumer) {
    exporterPositionColumnFamily.forEach(
        (exporterId, exporterStateEntry) ->
            consumer.accept(exporterId.toString(), exporterStateEntry));
  }

  public long getLowestPosition() {
    final LongArrayList positions = new LongArrayList();

    visitExporterState(
        (exporterId, exporterStateEntry) -> positions.addLong(exporterStateEntry.getPosition()));
    return positions.longStream().min().orElse(-1L);
  }

  public void removeExporterState(final String exporterId) {
    this.exporterId.wrapString(exporterId);
    exporterPositionColumnFamily.deleteIfExists(this.exporterId);
  }

  public boolean hasExporters() {
    return !exporterPositionColumnFamily.isEmpty();
  }
}
