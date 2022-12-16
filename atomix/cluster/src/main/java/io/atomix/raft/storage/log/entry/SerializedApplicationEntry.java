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
package io.atomix.raft.storage.log.entry;

import io.atomix.raft.storage.serializer.RaftEntrySerializer;
import io.atomix.raft.storage.serializer.RaftEntrySerializer.SerializedBufferWriterAdapter;
import io.camunda.zeebe.util.buffer.BufferWriter;
import io.camunda.zeebe.util.buffer.DirectBufferWriter;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Stores an entry that contains serialized records, ordered by their position; the lowestPosition
 * and highestPosition metadata allow for fast binary search over a collection of entries to quickly
 * find a particular record.
 */
public record SerializedApplicationEntry(
    long lowestPosition, long highestPosition, DirectBuffer data) implements ApplicationEntry {

  public SerializedApplicationEntry(
      final long lowestPosition, final long highestPosition, final ByteBuffer data) {
    this(lowestPosition, highestPosition, new UnsafeBuffer(data));
  }

  @Override
  public BufferWriter toSerializable(final long term, final RaftEntrySerializer serializer) {
    return new SerializedBufferWriterAdapter(
        () -> serializer.getApplicationEntrySerializedLength(this),
        (buffer, offset) -> serializer.writeApplicationEntry(term, this, buffer, offset));
  }

  @Override
  public BufferWriter dataWriter() {
    return new DirectBufferWriter().wrap(data);
  }
}
