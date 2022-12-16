/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.exporter.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.camunda.zeebe.broker.exporter.repo.ExporterLoadException;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.CONCURRENT)
final class ExporterContainerTest {

  private static final String EXPORTER_ID = "fakeExporter";

  private ExporterContainerRuntime runtime;
  private FakeExporter exporter;
  private ExporterContainer exporterContainer;

  @BeforeEach
  void beforeEach(final @TempDir Path storagePath) throws ExporterLoadException {
    runtime = new ExporterContainerRuntime(storagePath);

    final var descriptor =
        runtime.getRepository().load(EXPORTER_ID, FakeExporter.class, Map.of("key", "value"));
    exporterContainer = runtime.newContainer(descriptor);
    exporter = (FakeExporter) exporterContainer.getExporter();
  }

  @Test
  void shouldConfigureExporter() throws Exception {
    // given

    // when
    exporterContainer.configureExporter();

    // then
    assertThat(exporter.getContext()).isNotNull();
    assertThat(exporter.getContext().getLogger()).isNotNull();
    assertThat(exporter.getContext().getConfiguration()).isNotNull();
    assertThat(exporter.getContext().getConfiguration().getId()).isEqualTo(EXPORTER_ID);
    assertThat(exporter.getContext().getConfiguration().getArguments())
        .isEqualTo(Map.of("key", "value"));
  }

  @Test
  void shouldOpenExporter() throws Exception {
    // given
    exporterContainer.configureExporter();

    // when
    exporterContainer.openExporter();

    // then
    assertThat(exporter.getController()).isNotNull();
    assertThat(exporter.getController()).isEqualTo(exporterContainer);
  }

  @Test
  void shouldInitPositionToDefaultIfNotExistInState() throws Exception {
    // given
    exporterContainer.configureExporter();

    // when
    exporterContainer.initPosition();

    // then
    assertThat(exporterContainer.getPosition()).isEqualTo(-1);
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(-1);
  }

  @Test
  void shouldInitPositionWithStateValues() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0xCAFE);

    // when
    exporterContainer.initPosition();

    // then
    assertThat(exporterContainer.getPosition()).isEqualTo(0xCAFE);
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(0xCAFE);
  }

  @Test
  void shouldNotExportWhenRecordPositionIsSmaller() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0xCAFE);
    exporterContainer.initPosition();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();

    // when
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // then
    assertThat(exporter.getRecord()).isNull();
  }

  @Test
  void shouldUpdateUnacknowledgedPositionOnExport() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();

    // when
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // then
    assertThat(exporter.getRecord()).isNotNull();
    assertThat(exporter.getRecord()).isEqualTo(mockedRecord);
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(1);
    assertThat(exporterContainer.getPosition()).isEqualTo(0);
  }

  @Test
  void shouldUpdateUnacknowledgedPositionMultipleTimes() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // when
    final var secondRecord = mock(TypedRecord.class);
    when(secondRecord.getPosition()).thenReturn(2L);
    exporterContainer.exportRecord(recordMetadata, secondRecord);

    // then
    assertThat(exporter.getRecord()).isNotNull();
    assertThat(exporter.getRecord()).isEqualTo(secondRecord);
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(2);
    assertThat(exporterContainer.getPosition()).isEqualTo(0);
  }

  @Test
  void shouldUpdateExporterPosition() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();
    exporterContainer.openExporter();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // when
    exporterContainer.updateLastExportedRecordPosition(mockedRecord.getPosition());
    awaitPreviousCall();

    // then
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(1);
    assertThat(exporterContainer.getPosition()).isEqualTo(1);
    assertThat(runtime.getState().getPosition(EXPORTER_ID)).isEqualTo(1);
  }

  @Test
  void shouldNotUpdateExporterPositionToSmallerValue() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();
    exporterContainer.openExporter();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // when
    exporterContainer.updateLastExportedRecordPosition(-1);
    awaitPreviousCall();

    // then
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(1);
    assertThat(exporterContainer.getPosition()).isEqualTo(0);
    assertThat(runtime.getState().getPosition(EXPORTER_ID)).isEqualTo(0);
  }

  @Test
  void shouldNotUpdateExporterPositionInDifferentOrder() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();
    exporterContainer.openExporter();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();
    exporterContainer.exportRecord(recordMetadata, mockedRecord);
    when(mockedRecord.getPosition()).thenReturn(2L);
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // when
    exporterContainer.updateLastExportedRecordPosition(2);
    exporterContainer.updateLastExportedRecordPosition(1);
    awaitPreviousCall();

    // then
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(2);
    assertThat(exporterContainer.getPosition()).isEqualTo(2);
    assertThat(runtime.getState().getPosition(EXPORTER_ID)).isEqualTo(2);
  }

  @Test
  void shouldUpdatePositionsWhenRecordIsFiltered() throws Exception {
    // given
    exporterContainer.configureExporter();
    exporter.getContext().setFilter(new AlwaysRejectingFilter());
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();

    // when
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // then
    assertThat(exporter.getRecord()).isNull();
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(0);
    assertThat(exporterContainer.getPosition()).isEqualTo(1);
  }

  @Test
  void shouldUpdatePositionsWhenRecordIsFilteredAndPositionsAreEqual() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();

    final var mockedRecord = mock(TypedRecord.class);
    when(mockedRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();
    exporterContainer.exportRecord(recordMetadata, mockedRecord);
    exporterContainer.updateLastExportedRecordPosition(mockedRecord.getPosition());
    awaitPreviousCall();

    // when
    exporter.getContext().setFilter(new AlwaysRejectingFilter());
    when(mockedRecord.getPosition()).thenReturn(2L);
    exporterContainer.exportRecord(recordMetadata, mockedRecord);

    // then
    assertThat(exporter.getRecord()).isNotNull();
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(1);
    assertThat(exporterContainer.getPosition()).isEqualTo(2);
  }

  @Test
  void shouldNotUpdatePositionsWhenRecordIsFilteredAndLastEventWasUnacknowledged()
      throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();

    final var firstRecord = mock(TypedRecord.class);
    when(firstRecord.getPosition()).thenReturn(1L);
    final var recordMetadata = new RecordMetadata();
    exporterContainer.exportRecord(recordMetadata, firstRecord);

    // when
    final var secondRecord = mock(TypedRecord.class);
    when(secondRecord.getPosition()).thenReturn(2L);
    exporter.getContext().setFilter(new AlwaysRejectingFilter());
    exporterContainer.exportRecord(recordMetadata, secondRecord);

    // then
    assertThat(exporter.getRecord()).isNotNull();
    assertThat(exporter.getRecord()).isEqualTo(firstRecord);
    assertThat(exporterContainer.getLastUnacknowledgedPosition()).isEqualTo(1);
    assertThat(exporterContainer.getPosition()).isEqualTo(0);
  }

  @Test
  void shouldCloseExporter() throws Exception {
    // given
    exporterContainer.configureExporter();
    runtime.getState().setPosition(EXPORTER_ID, 0);
    exporterContainer.initPosition();

    // when
    exporterContainer.close();

    // then
    assertThat(exporter.isClosed()).isTrue();
  }

  @Test
  void shouldReturnEmptyMetadataIfNotExistInState() throws Exception {
    // given
    exporterContainer.configureExporter();

    // when
    final var metadata = exporterContainer.readMetadata();

    // then
    assertThat(metadata).isNotPresent();
  }

  @Test
  void shouldReadMetadataFromState() throws Exception {
    // given
    exporterContainer.configureExporter();

    final var metadata = "metadata".getBytes();
    runtime.getState().setExporterState(EXPORTER_ID, 10, BufferUtil.wrapArray(metadata));

    // when
    final var readMetadata = exporterContainer.readMetadata();

    // then
    assertThat(readMetadata).isPresent().hasValue(metadata);
  }

  @Test
  void shouldStoreMetadataInState() throws Exception {
    // given
    exporterContainer.configureExporter();

    // when
    final var metadata = "metadata".getBytes();
    exporterContainer.updateLastExportedRecordPosition(10, metadata);
    awaitPreviousCall();

    // then
    final var metadataInState = runtime.getState().getExporterMetadata(EXPORTER_ID);
    assertThat(metadataInState).isNotNull().isEqualTo(BufferUtil.wrapArray(metadata));
  }

  @Test
  void shouldNotUpdateMetadataInStateIfPositionIsSmaller() throws Exception {
    // given
    exporterContainer.configureExporter();

    final var metadataBefore = "m1".getBytes();
    exporterContainer.updateLastExportedRecordPosition(20, metadataBefore);
    awaitPreviousCall();

    // when
    final var metadataUpdated = "m2".getBytes();
    exporterContainer.updateLastExportedRecordPosition(10, metadataUpdated);
    awaitPreviousCall();

    // then
    final var metadataInState = runtime.getState().getExporterMetadata(EXPORTER_ID);
    assertThat(metadataInState).isNotNull().isEqualTo(BufferUtil.wrapArray(metadataBefore));
  }

  @Test
  void shouldStoreAndReadMetadata() throws Exception {
    // given
    exporterContainer.configureExporter();

    final var metadata = "metadata".getBytes();

    // when
    exporterContainer.updateLastExportedRecordPosition(10, metadata);
    awaitPreviousCall();

    final var readMetadata = exporterContainer.readMetadata();

    // then
    assertThat(readMetadata).isPresent().hasValue(metadata);
  }

  private void awaitPreviousCall() {
    // call is enqueued in queue and will be run after the previous call
    // when we await the call we can be sure that the previous call is also done
    runtime.getActor().getActorControl().call(() -> null).join();
  }

  public static final class FakeExporter implements Exporter {

    private Context context;
    private Controller controller;
    private Record<?> record;
    private boolean closed;

    public Context getContext() {
      return context;
    }

    public Controller getController() {
      return controller;
    }

    public Record<?> getRecord() {
      return record;
    }

    public boolean isClosed() {
      return closed;
    }

    @Override
    public void configure(final Context context) throws Exception {
      this.context = context;
    }

    @Override
    public void open(final Controller controller) {
      this.controller = controller;
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public void export(final Record<?> record) {
      this.record = record;
    }
  }

  private static final class AlwaysRejectingFilter implements Context.RecordFilter {

    @Override
    public boolean acceptType(final RecordType recordType) {
      return false;
    }

    @Override
    public boolean acceptValue(final ValueType valueType) {
      return false;
    }
  }
}
