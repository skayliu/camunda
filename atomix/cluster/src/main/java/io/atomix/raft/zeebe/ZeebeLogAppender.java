/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.atomix.raft.zeebe;

import io.atomix.raft.storage.log.IndexedRaftLogEntry;
import io.atomix.raft.storage.log.entry.ApplicationEntry;
import io.atomix.raft.storage.log.entry.SerializedApplicationEntry;
import io.atomix.raft.storage.log.entry.UnserializedApplicationEntry;
import io.camunda.zeebe.util.buffer.BufferWriter;
import java.nio.ByteBuffer;

/**
 * A log appender provides a central entry point to append to the local Raft log such that it is
 * automatically replicated and eventually committed, and the ability for callers to be notified of
 * various events, e.g. {@link AppendListener#onCommit(IndexedRaftLogEntry)}.
 */
@FunctionalInterface
public interface ZeebeLogAppender {
  /** Appends an entry to the local Raft log and schedules replication to each follower. */
  void appendEntry(ApplicationEntry entry, AppendListener appendListener);
  /**
   * Appends an entry to the local Raft log and schedules replication to each follower.
   *
   * @param lowestPosition lowest record position in the data buffer
   * @param highestPosition highest record position in the data buffer
   * @param data data to store in the entry
   */
  default void appendEntry(
      final long lowestPosition,
      final long highestPosition,
      final ByteBuffer data,
      final AppendListener appendListener) {
    appendEntry(
        new SerializedApplicationEntry(lowestPosition, highestPosition, data), appendListener);
  }

  /**
   * Appends an entry to the local Raft log and schedules replication to each follower.
   *
   * @param lowestPosition lowest record position in the data buffer
   * @param highestPosition highest record position in the data buffer
   * @param data data to store in the entry
   */
  default void appendEntry(
      final long lowestPosition,
      final long highestPosition,
      final BufferWriter data,
      final AppendListener appendListener) {
    appendEntry(
        new UnserializedApplicationEntry(lowestPosition, highestPosition, data), appendListener);
  }

  /**
   * An append listener can observe and be notified of different events related to the append
   * operation.
   */
  interface AppendListener {

    /**
     * Called when the entry has been written to the log.
     *
     * @param indexed the entry that was written to the log
     */
    default void onWrite(final IndexedRaftLogEntry indexed) {}

    /**
     * Called when an error occurred while writing the entry to the log.
     *
     * @param error the error that occurred
     */
    default void onWriteError(final Throwable error) {}

    /**
     * Called when the entry has been committed.
     *
     * @param indexed the entry that was committed
     */
    default void onCommit(final IndexedRaftLogEntry indexed) {}

    /**
     * Called when an error occurred while replicating or committing an entry, typically when if an
     * append operation was pending when shutting down the server or stepping down as leader.
     *
     * @param indexed the entry that should have been committed
     * @param error the error that occurred
     */
    default void onCommitError(final IndexedRaftLogEntry indexed, final Throwable error) {}
  }
}
