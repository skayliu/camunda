/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.migration.to_1_1;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.engine.state.immutable.ZeebeState;
import io.camunda.zeebe.engine.state.migration.MessageSubscriptionSentTimeMigration;
import io.camunda.zeebe.engine.state.mutable.MutableZeebeState;
import io.camunda.zeebe.engine.util.ZeebeStateExtension;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

public class MessageSubscriptionSentTimeMigrationTest {

  final MessageSubscriptionSentTimeMigration sutMigration =
      new MessageSubscriptionSentTimeMigration();

  @Nested
  public class MockBasedTests {

    @Test
    public void noMigrationNeededWhenColumnIsEmpty() {
      // given
      final var mockZeebeState = mock(ZeebeState.class);
      when(mockZeebeState.isEmpty(ZbColumnFamilies.MESSAGE_SUBSCRIPTION_BY_SENT_TIME))
          .thenReturn(true);
      // when
      final var actual = sutMigration.needsToRun(mockZeebeState);

      // then
      assertThat(actual).isFalse();
    }

    @Test
    public void migrationNeededWhenColumnIsNotEmpty() {
      // given
      final var mockZeebeState = mock(ZeebeState.class);
      when(mockZeebeState.isEmpty(ZbColumnFamilies.MESSAGE_SUBSCRIPTION_BY_SENT_TIME))
          .thenReturn(false);

      // when
      final var actual = sutMigration.needsToRun(mockZeebeState);

      // then
      assertThat(actual).isTrue();
    }

    @Test
    public void migrationCallsMethodInMigrationState() {
      // given
      final var mockZeebeState = mock(MutableZeebeState.class, RETURNS_DEEP_STUBS);

      // when
      sutMigration.runMigration(mockZeebeState);

      // then
      verify(mockZeebeState.getMigrationState())
          .migrateMessageSubscriptionSentTime(
              mockZeebeState.getMessageSubscriptionState(),
              mockZeebeState.getPendingMessageSubscriptionState());

      verifyNoMoreInteractions(mockZeebeState.getMigrationState());
    }
  }

  @Nested
  @ExtendWith(ZeebeStateExtension.class)
  public class BlackboxTest {

    private static final long TEST_SENT_TIME = 1000L;

    private ZeebeDb<ZbColumnFamilies> zeebeDb;

    private MutableZeebeState zeebeState;

    private TransactionContext transactionContext;

    @BeforeEach
    public void setUp() {
      final var legacySubscriptionState =
          new LegacyDbMessageSubscriptionState(zeebeDb, transactionContext);

      final LegacyMessageSubscription subscriptionInCorrelation =
          TestUtilities.createLegacyMessageSubscription(100, 1);
      legacySubscriptionState.put(
          subscriptionInCorrelation.getKey(), subscriptionInCorrelation.getRecord());
      legacySubscriptionState.updateSentTime(subscriptionInCorrelation, TEST_SENT_TIME);
    }

    @Test
    public void migrationNeedsToRun() {
      // given database with legacy records

      // when
      final var actual = sutMigration.needsToRun(zeebeState);

      // then
      assertThat(actual).describedAs("Migration should run").isTrue();
    }

    @Test
    public void afterMigrationRunNoFurtherMigrationIsNeeded() {
      // given database with legacy records

      // when
      sutMigration.runMigration(zeebeState);
      final var actual = sutMigration.needsToRun(zeebeState);

      // then
      assertThat(actual).describedAs("Migration should run").isFalse();
    }
  }
}
