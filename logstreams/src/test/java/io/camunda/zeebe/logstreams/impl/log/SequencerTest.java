/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.logstreams.impl.log;

import io.camunda.zeebe.logstreams.log.LogAppendEntry;
import io.camunda.zeebe.logstreams.util.TestEntry;
import io.camunda.zeebe.scheduler.ActorCondition;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

@SuppressWarnings("resource")
@Execution(ExecutionMode.CONCURRENT)
final class SequencerTest {

  @Test
  void notifiesConsumerOnWrite() {
    // given
    final var sequencer = new Sequencer(1, 0, 16);
    final var consumer = Mockito.mock(ActorCondition.class);

    // when
    sequencer.registerConsumer(consumer);
    sequencer.tryWrite(TestEntry.ofDefaults());

    // then
    Mockito.verify(consumer).signal();
  }

  @Test
  void notifiesConsumerOnBatchWrite() {
    // given
    final var sequencer = new Sequencer(1, 0, 16);
    final var consumer = Mockito.mock(ActorCondition.class);

    // when
    sequencer.registerConsumer(consumer);
    sequencer.tryWrite(List.of(TestEntry.ofDefaults(), TestEntry.ofDefaults()));

    // then
    Mockito.verify(consumer).signal();
  }

  @Test
  void canReadAfterSingleWrite() {
    // given
    final var sequencer = new Sequencer(1, 1, 16);
    final var entry = TestEntry.ofDefaults();

    // when
    sequencer.tryWrite(entry);

    // then
    final var read = sequencer.tryRead();
    Assertions.assertThat(read.entries()).containsExactly(entry);
  }

  @Test
  void canReadAfterBatchWrite() {
    // given
    final var sequencer = new Sequencer(1, 1, 16);
    final var entries =
        List.of(TestEntry.ofDefaults(), TestEntry.ofDefaults(), TestEntry.ofDefaults());

    // when
    sequencer.tryWrite(entries);

    // then
    final var read = sequencer.tryRead();
    Assertions.assertThat(read.entries()).containsAnyElementsOf(entries);
  }

  @Test
  void cannotReadEmpty() {
    // given
    final var sequencer = new Sequencer(1, 1, 16 * 1024 * 1024);

    // then
    final var read = sequencer.tryRead();
    Assertions.assertThat(read).isNull();
  }

  @Test
  void eventuallyRejectsWritesWithoutReader() {
    // given
    final var sequencer = new Sequencer(1, 1, 16 * 1024 * 1024);

    // then
    Awaitility.await("sequencer rejects writes")
        .pollInSameThread()
        .pollInterval(Duration.ZERO)
        .until(() -> sequencer.tryWrite(TestEntry.ofDefaults()), (result) -> result <= 0);
  }

  @Test
  void eventuallyRejectsBatchWritesWithoutReader() {
    // given
    final var sequencer = new Sequencer(1, 1, 16 * 1024 * 1024);

    // then
    Awaitility.await("sequencer rejects writes")
        .pollInSameThread()
        .pollInterval(Duration.ZERO)
        .until(
            () -> sequencer.tryWrite(List.of(TestEntry.ofKey(1), TestEntry.ofKey(2))),
            (result) -> result <= 0);
  }

  @Test
  void writingSingleEntryIncreasesPositions() {
    // given
    final var initialPosition = 1;
    final var sequencer = new Sequencer(1, initialPosition, 16 * 1024 * 1024);

    // when
    final var result = sequencer.tryWrite(TestEntry.ofDefaults());

    // then
    Assertions.assertThat(result).isPositive().isEqualTo(initialPosition);
  }

  @Test
  void writingMultipleEntriesIncreasesPositions() {
    // given
    final var initialPosition = 1;
    final var sequencer = new Sequencer(1, initialPosition, 16 * 1024 * 1024);
    final var entries =
        List.of(TestEntry.ofDefaults(), TestEntry.ofDefaults(), TestEntry.ofDefaults());
    // when
    final var result = sequencer.tryWrite(entries);

    // then
    Assertions.assertThat(result).isPositive().isEqualTo(initialPosition + entries.size() - 1);
  }

  @Test
  void notifiesReaderWhenRejectingWriteDueToFullQueue() {
    // given
    final var sequencer = new Sequencer(1, 1, 16 * 1024 * 1024);
    Awaitility.await("sequencer rejects writes")
        .pollInSameThread()
        .pollInterval(Duration.ZERO)
        .until(() -> sequencer.tryWrite(TestEntry.ofDefaults()), (result) -> result <= 0);
    final var consumer = Mockito.mock(ActorCondition.class);

    // when
    sequencer.registerConsumer(consumer);
    final var result = sequencer.tryWrite(TestEntry.ofDefaults());

    // then
    Assertions.assertThat(result).isNegative();
    Mockito.verify(consumer).signal();
  }

  @Test
  void notifiesReaderWhenRejectingBatchWriteDueToFullQueue() {
    // given
    final var sequencer = new Sequencer(1, 1, 16 * 1024 * 1024);
    Awaitility.await("sequencer rejects writes")
        .pollInSameThread()
        .pollInterval(Duration.ZERO)
        .until(() -> sequencer.tryWrite(TestEntry.ofDefaults()), (result) -> result <= 0);
    final var consumer = Mockito.mock(ActorCondition.class);

    // when
    sequencer.registerConsumer(consumer);
    final var result = sequencer.tryWrite(List.of(TestEntry.ofKey(1), TestEntry.ofKey(2)));

    // then
    Assertions.assertThat(result).isNegative();
    Mockito.verify(consumer).signal();
  }

  @Test
  void keepsPositionsWithSingleWriter() throws InterruptedException {
    // given
    final var initialPosition = 1L;
    final var entriesToWrite = 10_000L;
    final var sequencer = new Sequencer(1, initialPosition, 16 * 1024 * 1024);
    final var batch = List.of(TestEntry.ofKey(1));
    final var reader = newReaderThread(sequencer, initialPosition, entriesToWrite);
    final var writer = newWriterThread(sequencer, initialPosition, entriesToWrite, batch, true);

    // when
    reader.start();
    writer.start();

    // then -- readers and writers don't throw
    reader.join(10 * 1000);
    writer.join(10 * 1000);
  }

  @Test
  void keepsPositionsWithMultipleWriters() throws InterruptedException {
    // given
    final var writers = 3;

    final var initialPosition = 1L;
    final var entriesToWrite = 10_000L;
    final var entriesToRead = writers * entriesToWrite;
    final var sequencer = new Sequencer(1, initialPosition, 16 * 1024 * 1024);
    final var reader = newReaderThread(sequencer, initialPosition, entriesToRead);
    final var batch = List.of(TestEntry.ofKey(1));
    final var writerThreads =
        IntStream.range(0, writers)
            .mapToObj(
                i -> newWriterThread(sequencer, initialPosition, entriesToWrite, batch, false))
            .toList();

    // when
    reader.start();
    writerThreads.forEach(Thread::start);

    // then -- readers and writers don't throw
    reader.join(10 * 1000);
    writerThreads.forEach(
        thread -> {
          try {
            thread.join(10 * 1000);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  void keepsPositionsWithMultipleWritersWritingMultipleEntries() throws InterruptedException {
    // given
    final var writers = 3;

    final var initialPosition = 1L;
    final var batchesToWrite = 10_000L;
    final var batchesToRead = writers * batchesToWrite;
    final var sequencer = new Sequencer(1, initialPosition, 16 * 1024 * 1024);
    final var reader = newReaderThread(sequencer, initialPosition, batchesToRead);
    final var batch =
        List.of(TestEntry.ofKey(1), TestEntry.ofKey(1), TestEntry.ofKey(1), TestEntry.ofKey(1));
    final var writerThreads =
        IntStream.range(0, writers)
            .mapToObj(
                i -> newWriterThread(sequencer, initialPosition, batchesToWrite, batch, false))
            .toList();

    // when
    reader.start();
    writerThreads.forEach(Thread::start);

    // then -- readers and writers don't throw and eventually finish
    reader.join(10 * 1000);
    writerThreads.forEach(
        thread -> {
          try {
            thread.join(10 * 1000);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private Thread newReaderThread(
      final Sequencer sequencer, final long initialPosition, final long batchesToRead) {
    return new Thread(
        () -> {
          var batchesRead = 0L;
          var lastReadPosition = initialPosition - 1;
          while (batchesRead < batchesToRead) {
            final var result = sequencer.tryRead();
            if (result != null) {
              Assertions.assertThat(result.firstPosition()).isEqualTo(lastReadPosition + 1);
              lastReadPosition = result.firstPosition() + result.entries().size() - 1;
              batchesRead += 1;
            }
          }
        });
  }

  private Thread newWriterThread(
      final Sequencer sequencer,
      final long initialPosition,
      final long batchesToWrite,
      final List<LogAppendEntry> batchToWrite,
      final boolean isOnlyWriter) {
    return new Thread(
        () -> {
          var batchesWritten = 0L;
          var lastWrittenPosition = initialPosition - 1;
          while (batchesWritten < batchesToWrite) {
            final var result = sequencer.tryWrite(batchToWrite);
            if (result > 0) {
              if (isOnlyWriter) {
                Assertions.assertThat(result).isEqualTo(lastWrittenPosition + batchToWrite.size());
              } else {
                Assertions.assertThat(result).isGreaterThan(lastWrittenPosition);
              }
              lastWrittenPosition = result;
              batchesWritten += 1;
            } else {
              LockSupport.parkNanos(1_000_000);
            }
          }
        });
  }
}
