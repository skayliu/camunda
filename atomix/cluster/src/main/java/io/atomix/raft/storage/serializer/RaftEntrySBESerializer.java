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
package io.atomix.raft.storage.serializer;

import static io.atomix.raft.storage.serializer.ConfigurationEntryEncoder.RaftMemberEncoder;
import static io.atomix.raft.storage.serializer.SerializerUtil.getRaftMemberType;
import static io.atomix.raft.storage.serializer.SerializerUtil.getSBEType;

import io.atomix.cluster.MemberId;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.cluster.impl.DefaultRaftMember;
import io.atomix.raft.storage.log.entry.ApplicationEntry;
import io.atomix.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.raft.storage.log.entry.InitialEntry;
import io.atomix.raft.storage.log.entry.RaftEntry;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.storage.log.entry.SerializedApplicationEntry;
import io.atomix.raft.storage.serializer.ConfigurationEntryDecoder.RaftMemberDecoder;
import io.camunda.zeebe.journal.file.RecordDataEncoder;
import java.time.Instant;
import java.util.ArrayList;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class RaftEntrySBESerializer implements RaftEntrySerializer {
  final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  final RaftLogEntryEncoder raftLogEntryEncoder = new RaftLogEntryEncoder();
  final ApplicationEntryEncoder applicationEntryEncoder = new ApplicationEntryEncoder();
  final ConfigurationEntryEncoder configurationEntryEncoder = new ConfigurationEntryEncoder();
  final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  final RaftLogEntryDecoder raftLogEntryDecoder = new RaftLogEntryDecoder();
  final ApplicationEntryDecoder applicationEntryDecoder = new ApplicationEntryDecoder();
  final ConfigurationEntryDecoder configurationEntryDecoder = new ConfigurationEntryDecoder();

  @Override
  public int getApplicationEntrySerializedLength(final ApplicationEntry entry) {
    // raft frame length
    return headerEncoder.encodedLength()
        + raftLogEntryEncoder.sbeBlockLength()
        // + application entry length
        + headerEncoder.encodedLength()
        + applicationEntryEncoder.sbeBlockLength()
        + RecordDataEncoder.dataHeaderLength()
        + entry.dataWriter().getLength();
  }

  @Override
  public int getInitialEntrySerializedLength() {
    return headerEncoder.encodedLength() + raftLogEntryEncoder.sbeBlockLength();
  }

  @Override
  public int getConfigurationEntrySerializedLength(final ConfigurationEntry entry) {
    // raft frame length
    return headerEncoder.encodedLength()
        + raftLogEntryEncoder.sbeBlockLength()
        // configuration entry length
        + headerEncoder.encodedLength()
        // timestamp
        + configurationEntryEncoder.sbeBlockLength()
        // members header
        + RaftMemberEncoder.sbeHeaderSize()
        // member entries
        + entry.members().stream()
            .map(this::getConfigurationMemberEntryLength)
            .mapToInt(Integer::intValue)
            .sum();
  }

  @Override
  public int writeApplicationEntry(
      final long term,
      final ApplicationEntry entry,
      final MutableDirectBuffer buffer,
      final int offset) {

    final int entryOffset = writeRaftFrame(term, EntryType.ApplicationEntry, buffer, offset);

    headerEncoder
        .wrap(buffer, offset + entryOffset)
        .blockLength(applicationEntryEncoder.sbeBlockLength())
        .templateId(applicationEntryEncoder.sbeTemplateId())
        .schemaId(applicationEntryEncoder.sbeSchemaId())
        .version(applicationEntryEncoder.sbeSchemaVersion());
    applicationEntryEncoder.wrap(buffer, offset + entryOffset + headerEncoder.encodedLength());
    applicationEntryEncoder.lowestAsqn(entry.lowestPosition()).highestAsqn(entry.highestPosition());

    // Re-implements `ApplicationEntryEncoder.putApplicationData`
    final var dataWriter = entry.dataWriter();
    final var dataLength = dataWriter.getLength();
    final int headerLength = ApplicationEntryEncoder.applicationDataHeaderLength();
    final int limit = applicationEntryEncoder.limit();
    applicationEntryEncoder.limit(limit + headerLength + dataLength);
    buffer.putInt(limit, dataLength, java.nio.ByteOrder.LITTLE_ENDIAN);
    dataWriter.write(applicationEntryEncoder.buffer(), limit + headerLength);

    return entryOffset + headerEncoder.encodedLength() + applicationEntryEncoder.encodedLength();
  }

  @Override
  public int writeInitialEntry(
      final long term,
      final InitialEntry entry,
      final MutableDirectBuffer buffer,
      final int offset) {

    return writeRaftFrame(term, EntryType.InitialEntry, buffer, offset);
  }

  @Override
  public int writeConfigurationEntry(
      final long term,
      final ConfigurationEntry entry,
      final MutableDirectBuffer buffer,
      final int offset) {
    final int entryOffset = writeRaftFrame(term, EntryType.ConfigurationEntry, buffer, offset);

    headerEncoder
        .wrap(buffer, offset + entryOffset)
        .blockLength(configurationEntryEncoder.sbeBlockLength())
        .templateId(configurationEntryEncoder.sbeTemplateId())
        .schemaId(configurationEntryEncoder.sbeSchemaId())
        .version(configurationEntryEncoder.sbeSchemaVersion());

    configurationEntryEncoder.wrap(buffer, offset + entryOffset + headerEncoder.encodedLength());

    configurationEntryEncoder.timestamp(entry.timestamp());

    final var raftMemberEncoder = configurationEntryEncoder.raftMemberCount(entry.members().size());
    for (final RaftMember member : entry.members()) {
      final var memberId = member.memberId().id();
      raftMemberEncoder
          .next()
          .type(getSBEType(member.getType()))
          .updated(member.getLastUpdated().toEpochMilli())
          .memberId(memberId);
    }

    return entryOffset + headerEncoder.encodedLength() + configurationEntryEncoder.encodedLength();
  }

  @Override
  public RaftLogEntry readRaftLogEntry(final DirectBuffer buffer) {
    headerDecoder.wrap(buffer, 0);
    raftLogEntryDecoder.wrap(
        buffer,
        headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());
    final long term = raftLogEntryDecoder.term();
    final EntryType type = raftLogEntryDecoder.type();

    final RaftEntry entry;
    final int entryOffset = headerDecoder.encodedLength() + raftLogEntryDecoder.encodedLength();

    switch (type) {
      case ApplicationEntry:
        headerDecoder.wrap(buffer, entryOffset);
        entry = readApplicationEntry(buffer, entryOffset);
        break;
      case ConfigurationEntry:
        headerDecoder.wrap(buffer, entryOffset);
        entry = readConfigurationEntry(buffer, entryOffset);
        break;
      case InitialEntry:
        entry = new InitialEntry();
        break;
      default:
        throw new IllegalStateException("Unexpected entry type " + type);
    }

    return new RaftLogEntry(term, entry);
  }

  private int getConfigurationMemberEntryLength(final RaftMember raftMember) {
    final String id = raftMember.memberId().id();
    return RaftMemberEncoder.sbeBlockLength()
        + RaftMemberEncoder.memberIdHeaderLength()
        + ((null == id || id.isEmpty()) ? 0 : id.length());
  }

  private int writeRaftFrame(
      final long term,
      final EntryType entryType,
      final MutableDirectBuffer buffer,
      final int offset) {
    headerEncoder
        .wrap(buffer, offset)
        .blockLength(raftLogEntryEncoder.sbeBlockLength())
        .templateId(raftLogEntryEncoder.sbeTemplateId())
        .schemaId(raftLogEntryEncoder.sbeSchemaId())
        .version(raftLogEntryEncoder.sbeSchemaVersion());
    raftLogEntryEncoder.wrap(buffer, offset + headerEncoder.encodedLength());
    raftLogEntryEncoder.term(term);
    raftLogEntryEncoder.type(entryType);

    return headerEncoder.encodedLength() + raftLogEntryEncoder.encodedLength();
  }

  private ApplicationEntry readApplicationEntry(final DirectBuffer buffer, final int entryOffset) {
    applicationEntryDecoder.wrap(
        buffer,
        entryOffset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final DirectBuffer data = new UnsafeBuffer();
    applicationEntryDecoder.wrapApplicationData(data);

    return new SerializedApplicationEntry(
        applicationEntryDecoder.lowestAsqn(), applicationEntryDecoder.highestAsqn(), data);
  }

  private ConfigurationEntry readConfigurationEntry(
      final DirectBuffer buffer, final int entryOffset) {

    configurationEntryDecoder.wrap(
        buffer,
        entryOffset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final long timestamp = configurationEntryDecoder.timestamp();

    final RaftMemberDecoder memberDecoder = configurationEntryDecoder.raftMember();
    final ArrayList<RaftMember> members = new ArrayList<>(memberDecoder.count());
    for (final RaftMemberDecoder member : memberDecoder) {
      final RaftMember.Type type = getRaftMemberType(member.type());
      final Instant updated = Instant.ofEpochMilli(member.updated());
      final var memberId = member.memberId();
      members.add(new DefaultRaftMember(MemberId.from(memberId), type, updated));
    }

    return new ConfigurationEntry(timestamp, members);
  }
}
