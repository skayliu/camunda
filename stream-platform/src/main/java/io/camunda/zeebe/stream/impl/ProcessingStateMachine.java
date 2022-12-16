/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.stream.impl;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDbTransaction;
import io.camunda.zeebe.logstreams.impl.Loggers;
import io.camunda.zeebe.logstreams.log.LogStreamReader;
import io.camunda.zeebe.logstreams.log.LogStreamWriter;
import io.camunda.zeebe.logstreams.log.LoggedEvent;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.scheduler.ActorControl;
import io.camunda.zeebe.scheduler.clock.ActorClock;
import io.camunda.zeebe.scheduler.future.ActorFuture;
import io.camunda.zeebe.scheduler.retry.AbortableRetryStrategy;
import io.camunda.zeebe.scheduler.retry.RecoverableRetryStrategy;
import io.camunda.zeebe.scheduler.retry.RetryStrategy;
import io.camunda.zeebe.stream.api.EmptyProcessingResult;
import io.camunda.zeebe.stream.api.EventFilter;
import io.camunda.zeebe.stream.api.MetadataFilter;
import io.camunda.zeebe.stream.api.ProcessingResult;
import io.camunda.zeebe.stream.api.ProcessingResultBuilder;
import io.camunda.zeebe.stream.api.RecordProcessor;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.MutableLastProcessedPositionState;
import io.camunda.zeebe.stream.impl.metrics.StreamProcessorMetrics;
import io.camunda.zeebe.stream.impl.records.RecordValues;
import io.camunda.zeebe.stream.impl.records.TypedRecordImpl;
import io.camunda.zeebe.util.buffer.BufferUtil;
import io.camunda.zeebe.util.exception.RecoverableException;
import io.camunda.zeebe.util.exception.UnrecoverableException;
import io.prometheus.client.Histogram;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;

/**
 * Represents the processing state machine, which is executed on normal processing.
 *
 * <pre>
 *
 * +------------------+            +--------------------+
 * |                  |            |                    |      exception
 * | readNextRecord() |----------->|  processCommand()  |------------------+
 * |                  |            |                    |                  v
 * +------------------+            +--------------------+            +---------------+
 *           ^                             |                         |               |------+
 *           |                             |         +-------------->|   onError()   |      | exception
 *           |                             |         |  exception    |               |<-----+
 *           |                     +-------v-------------+           +---------------+
 *           |                     |                     |                 |
 *           |                     |   writeRecords()    |                 |
 *           |                     |                     |<----------------+
 * +----------------------+        +---------------------+
 * |                      |                 |
 * | executeSideEffects() |                 v
 * |                      |       +----------------------+
 * +----------------------+       |                      |
 *           ^                    |     updateState()    |
 *           +--------------------|                      |
 *                                +----------------------+
 *                                       ^      |
 *                                       |      | exception
 *                                       |      |
 *                                    +---------v----+
 *                                    |              |
 *                                    |   onError()  |
 *                                    |              |
 *                                    +--------------+
 *                                       ^     |
 *                                       |     |  exception
 *                                       +-----+
 *
 * </pre>
 */
public final class ProcessingStateMachine {

  private static final Logger LOG = Loggers.PROCESSOR_LOGGER;
  private static final String ERROR_MESSAGE_WRITE_RECORD_ABORTED =
      "Expected to write one or more follow-up records for record '{} {}' without errors, but exception was thrown.";
  private static final String ERROR_MESSAGE_ROLLBACK_ABORTED =
      "Expected to roll back the current transaction for record '{} {}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED =
      "Expected to execute side effects for record '{} {}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_UPDATE_STATE_FAILED =
      "Expected to successfully update state for record '{} {}', but caught an exception. Retry.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING =
      "Expected to process record '{} {}' successfully on stream processor, but caught recoverable exception. Retry processing.";
  private static final String NOTIFY_PROCESSED_LISTENER_ERROR_MESSAGE =
      "Expected to invoke processed listener for record {} successfully, but exception was thrown.";
  private static final String NOTIFY_SKIPPED_LISTENER_ERROR_MESSAGE =
      "Expected to invoke skipped listener for record '{} {}' successfully, but exception was thrown.";

  private static final Duration PROCESSING_RETRY_DELAY = Duration.ofMillis(250);

  private static final MetadataFilter PROCESSING_FILTER =
      recordMetadata -> recordMetadata.getRecordType() == RecordType.COMMAND;

  private final EventFilter eventFilter =
      new MetadataEventFilter(new RecordProtocolVersionFilter().and(PROCESSING_FILTER));

  private final EventFilter commandFilter =
      new MetadataEventFilter(
          recordMetadata -> recordMetadata.getRecordType() != RecordType.COMMAND);

  private final MutableLastProcessedPositionState lastProcessedPositionState;
  private final RecordMetadata metadata = new RecordMetadata();
  private final ActorControl actor;
  private final LogStreamReader logStreamReader;
  private final TransactionContext transactionContext;
  private final RetryStrategy writeRetryStrategy;
  private final RetryStrategy sideEffectsRetryStrategy;
  private final RetryStrategy updateStateRetryStrategy;
  private final BooleanSupplier shouldProcessNext;
  private final BooleanSupplier abortCondition;
  private final RecordValues recordValues;
  private final TypedRecordImpl typedCommand;
  private final StreamProcessorMetrics metrics;
  private final StreamProcessorListener streamProcessorListener;

  // current iteration
  private LoggedEvent currentRecord;
  private ZeebeDbTransaction zeebeDbTransaction;
  private long writtenPosition = StreamProcessor.UNSET_POSITION;
  private long lastSuccessfulProcessedRecordPosition = StreamProcessor.UNSET_POSITION;
  private long lastWrittenPosition = StreamProcessor.UNSET_POSITION;
  private volatile boolean onErrorHandlingLoop;
  private int onErrorRetries;
  // Used for processing duration metrics
  private Histogram.Timer processingTimer;
  private boolean reachedEnd = true;
  private final StreamProcessorContext context;
  private final List<RecordProcessor> recordProcessors;
  private ProcessingResult currentProcessingResult;
  private RecordProcessor currentProcessor;
  private final LogStreamWriter logStreamWriter;
  private boolean inProcessing;

  public ProcessingStateMachine(
      final StreamProcessorContext context,
      final BooleanSupplier shouldProcessNext,
      final List<RecordProcessor> recordProcessors) {
    this.context = context;
    this.recordProcessors = recordProcessors;
    actor = context.getActor();
    recordValues = context.getRecordValues();
    logStreamReader = context.getLogStreamReader();
    logStreamWriter = context.getLogStreamWriter();
    transactionContext = context.getTransactionContext();
    abortCondition = context.getAbortCondition();
    lastProcessedPositionState = context.getLastProcessedPositionState();

    writeRetryStrategy = new AbortableRetryStrategy(actor);
    sideEffectsRetryStrategy = new AbortableRetryStrategy(actor);
    updateStateRetryStrategy = new RecoverableRetryStrategy(actor);
    this.shouldProcessNext = shouldProcessNext;

    final int partitionId = context.getLogStream().getPartitionId();
    typedCommand = new TypedRecordImpl(partitionId);

    metrics = new StreamProcessorMetrics(partitionId);
    streamProcessorListener = context.getStreamProcessorListener();
  }

  private void skipRecord() {
    notifySkippedListener(currentRecord);
    inProcessing = false;
    actor.submit(this::readNextRecord);
    metrics.eventSkipped();
  }

  void readNextRecord() {
    if (onErrorRetries > 0) {
      onErrorHandlingLoop = false;
      onErrorRetries = 0;
    }

    tryToReadNextRecord();
  }

  private void tryToReadNextRecord() {
    final var hasNext = logStreamReader.hasNext();

    if (currentRecord != null) {
      final var previousRecord = currentRecord;
      // All commands cause a follow-up event or rejection, which means the processor
      // reached the end of the log if:
      //  * the last record was an event or rejection
      //  * and there is no next record on the log
      //  * and this was the last record written (records that have been written to the dispatcher
      //    might not be written to the log yet, which means they will appear shortly after this)
      reachedEnd =
          commandFilter.applies(previousRecord)
              && !hasNext
              && lastWrittenPosition <= previousRecord.getPosition();
    }

    if (shouldProcessNext.getAsBoolean() && hasNext && !inProcessing) {
      currentRecord = logStreamReader.next();

      if (eventFilter.applies(currentRecord)) {
        processCommand(currentRecord);
      } else {
        skipRecord();
      }
    }
  }

  /**
   * Be aware this is a transient property which can change anytime, e.g. if a new command is
   * written to the log.
   *
   * @return true if the ProcessingStateMachine has reached the end of the log and nothing is left
   *     to being processed/applied, false otherwise
   */
  public boolean hasReachedEnd() {
    return reachedEnd;
  }

  private void processCommand(final LoggedEvent command) {
    // we have to mark ourself has inProcessing to not interfere with readNext calls, which
    // are triggered from commit listener
    inProcessing = true;

    currentProcessingResult = EmptyProcessingResult.INSTANCE;

    metadata.reset();
    command.readMetadata(metadata);

    // Here we need to get the current time, since we want to calculate
    // how long it took between writing to the dispatcher and processing.
    // In all other cases we should prefer to use the Prometheus Timer API.
    final var processingStartTime = ActorClock.currentTimeMillis();
    processingTimer = metrics.startProcessingDurationTimer(metadata.getRecordType());

    try {
      final var value = recordValues.readRecordValue(command, metadata.getValueType());
      typedCommand.wrap(command, metadata, value);

      final long position = typedCommand.getPosition();
      final ProcessingResultBuilder processingResultBuilder =
          new BufferedProcessingResultBuilder(logStreamWriter::canWriteEvents);

      metrics.processingLatency(command.getTimestamp(), processingStartTime);

      currentProcessor =
          recordProcessors.stream()
              .filter(p -> p.accepts(typedCommand.getValueType()))
              .findFirst()
              .orElse(null);

      zeebeDbTransaction = transactionContext.getCurrentTransaction();
      zeebeDbTransaction.run(
          () -> {
            if (currentProcessor != null) {
              currentProcessingResult =
                  currentProcessor.process(typedCommand, processingResultBuilder);
            }

            lastProcessedPositionState.markAsProcessed(position);
          });

      metrics.commandsProcessed();

      if (EmptyProcessingResult.INSTANCE == currentProcessingResult) {
        skipRecord();
        return;
      }

      writeRecords();
    } catch (final RecoverableException recoverableException) {
      // recoverable
      LOG.error(
          ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING,
          command,
          metadata,
          recoverableException);
      actor.runDelayed(PROCESSING_RETRY_DELAY, () -> processCommand(currentRecord));
    } catch (final UnrecoverableException unrecoverableException) {
      throw unrecoverableException;
    } catch (final Exception e) {
      onError(e, this::writeRecords);
    }
  }

  private void onError(final Throwable processingException, final Runnable nextStep) {
    onErrorRetries++;
    if (onErrorRetries > 1) {
      onErrorHandlingLoop = true;
    }
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.rollback();
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_ROLLBACK_ABORTED, currentRecord, metadata, throwable);
          }
          try {
            errorHandlingInTransaction(processingException);

            nextStep.run();
          } catch (final Exception ex) {
            onError(ex, nextStep);
          }
        });
  }

  private void errorHandlingInTransaction(final Throwable processingException) throws Exception {
    zeebeDbTransaction = transactionContext.getCurrentTransaction();
    zeebeDbTransaction.run(
        () -> {
          final ProcessingResultBuilder processingResultBuilder =
              new BufferedProcessingResultBuilder(logStreamWriter::canWriteEvents);

          currentProcessingResult =
              currentProcessor.onProcessingError(
                  processingException, typedCommand, processingResultBuilder);
        });
  }

  private void writeRecords() {
    final var sourceRecordPosition = typedCommand.getPosition();

    final ActorFuture<Boolean> retryFuture =
        writeRetryStrategy.runWithRetry(
            () -> {
              final long position =
                  logStreamWriter.tryWrite(
                      currentProcessingResult.getRecordBatch().entries(), sourceRecordPosition);
              if (position > 0) {
                writtenPosition = position;
              }
              return position >= 0;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, t) -> {
          if (t != null) {
            LOG.error(ERROR_MESSAGE_WRITE_RECORD_ABORTED, currentRecord, metadata, t);
            onError(t, this::writeRecords);
          } else {
            // We write various type of records. The positions are always increasing and
            // incremented by 1 for one record (even in a batch), so we can count the amount
            // of written records via the lastWritten and now written position.
            final var amount = writtenPosition - lastWrittenPosition;
            metrics.recordsWritten(amount);
            updateState();
          }
        });
  }

  private void updateState() {
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.commit();
              lastSuccessfulProcessedRecordPosition = currentRecord.getPosition();
              metrics.setLastProcessedPosition(lastSuccessfulProcessedRecordPosition);
              lastWrittenPosition = writtenPosition;
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_UPDATE_STATE_FAILED, currentRecord, metadata, throwable);
            onError(throwable, this::updateState);
          } else {
            executeSideEffects();
          }
        });
  }

  private void executeSideEffects() {
    final ActorFuture<Boolean> retryFuture =
        sideEffectsRetryStrategy.runWithRetry(
            () -> {
              // TODO refactor this into two parallel tasks, which are then combined, and on the
              // completion of which the process continues

              final var processingResponseOptional =
                  currentProcessingResult.getProcessingResponse();

              if (processingResponseOptional.isPresent()) {
                final var processingResponse = processingResponseOptional.get();
                final var responseWriter = context.getCommandResponseWriter();

                final var responseValue = processingResponse.responseValue();
                final var recordMetadata = responseValue.recordMetadata();
                final boolean responseSent =
                    responseWriter
                        .intent(recordMetadata.getIntent())
                        .key(responseValue.key())
                        .recordType(recordMetadata.getRecordType())
                        .rejectionReason(BufferUtil.wrapString(recordMetadata.getRejectionReason()))
                        .rejectionType(recordMetadata.getRejectionType())
                        .partitionId(context.getPartitionId())
                        .valueType(recordMetadata.getValueType())
                        .valueWriter(responseValue.recordValue())
                        .tryWriteResponse(
                            processingResponse.requestStreamId(), processingResponse.requestId());
                if (!responseSent) {
                  return false;
                } else {
                  return currentProcessingResult.executePostCommitTasks();
                }
              }
              return currentProcessingResult.executePostCommitTasks();
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(
                ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED, currentRecord, metadata, throwable);
          }

          notifyProcessedListener(typedCommand);

          // observe the processing duration
          processingTimer.close();

          // continue with next record
          inProcessing = false;
          actor.submit(this::readNextRecord);
        });
  }

  private void notifyProcessedListener(final TypedRecord processedRecord) {
    try {
      streamProcessorListener.onProcessed(processedRecord);
    } catch (final Exception e) {
      LOG.error(NOTIFY_PROCESSED_LISTENER_ERROR_MESSAGE, processedRecord, e);
    }
  }

  private void notifySkippedListener(final LoggedEvent skippedRecord) {
    try {
      streamProcessorListener.onSkipped(skippedRecord);
    } catch (final Exception e) {
      LOG.error(NOTIFY_SKIPPED_LISTENER_ERROR_MESSAGE, skippedRecord, metadata, e);
    }
  }

  public long getLastSuccessfulProcessedRecordPosition() {
    return lastSuccessfulProcessedRecordPosition;
  }

  public long getLastWrittenPosition() {
    return lastWrittenPosition;
  }

  public boolean isMakingProgress() {
    return !onErrorHandlingLoop;
  }

  public void startProcessing(final LastProcessingPositions lastProcessingPositions) {
    // Replay ends at the end of the log and returns the lastSourceRecordPosition
    // which is equal to the last processed position
    // we need to seek to the next record after that position where the processing should start
    // Be aware on processing we ignore events, so we will process the next command
    final var lastProcessedPosition = lastProcessingPositions.getLastProcessedPosition();
    logStreamReader.seekToNextEvent(lastProcessedPosition);
    if (lastSuccessfulProcessedRecordPosition == StreamProcessor.UNSET_POSITION) {
      lastSuccessfulProcessedRecordPosition = lastProcessedPosition;
    }

    if (lastWrittenPosition == StreamProcessor.UNSET_POSITION) {
      lastWrittenPosition = lastProcessingPositions.getLastWrittenPosition();
    }

    actor.submit(this::readNextRecord);
  }
}
