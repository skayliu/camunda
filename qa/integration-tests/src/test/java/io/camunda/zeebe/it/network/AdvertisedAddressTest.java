/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.network;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.qa.util.testcontainers.ProxyRegistry;
import io.camunda.zeebe.qa.util.testcontainers.ProxyRegistry.ContainerProxy;
import io.camunda.zeebe.qa.util.testcontainers.ZeebeTestContainerDefaults;
import io.camunda.zeebe.test.util.asserts.TopologyAssert;
import io.zeebe.containers.ZeebeBrokerNode;
import io.zeebe.containers.ZeebeGatewayNode;
import io.zeebe.containers.cluster.ZeebeCluster;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Use Toxiproxy. Even though it does not support UDP, we can still use Toxiproxy because only
 * "gossip" messages use UDP. SWIM has other messages to probe and sync that uses TCP. So the
 * brokers can still find each other.
 */
@Testcontainers
final class AdvertisedAddressTest {
  private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
  private static final String TOXIPROXY_IMAGE = "shopify/toxiproxy:2.1.0";

  private final Network network = Network.newNetwork();

  @Container
  private final ToxiproxyContainer toxiproxy =
      new ToxiproxyContainer(DockerImageName.parse(TOXIPROXY_IMAGE))
          .withNetwork(network)
          .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

  private final ProxyRegistry proxyRegistry = new ProxyRegistry(toxiproxy);

  private final List<String> initialContactPoints = new ArrayList<>();
  private final ZeebeCluster cluster =
      ZeebeCluster.builder()
          .withImage(ZeebeTestContainerDefaults.defaultTestImage())
          .withNetwork(network)
          .withEmbeddedGateway(false)
          .withGatewaysCount(1)
          .withBrokersCount(3)
          .withPartitionsCount(1)
          .withReplicationFactor(3)
          .build();

  @BeforeEach
  void beforeEach() {
    cluster.getBrokers().values().forEach(this::configureBroker);
    cluster.getGateways().values().forEach(this::configureGateway);

    // the first pass of configureBroker builds up the initial contact points; this has to be done
    // as we're also creating the proxies. we use a second pass such that all nodes known about all
    // others during bootstrapping.
    cluster
        .getBrokers()
        .values()
        .forEach(
            broker ->
                broker.withEnv(
                    "ZEEBE_BROKER_CLUSTER_INITIALCONTACTPOINTS",
                    String.join(",", initialContactPoints)));
  }

  @AfterEach
  void afterEach() {
    CloseHelper.quietCloseAll(cluster, network);
  }

  @Test
  void shouldCommunicateOverProxy() {
    // given
    cluster.start();

    // when - send a message to verify the gateway can talk to the broker directly not just via
    // gossip
    try (final var client = cluster.newClientBuilder().build()) {
      final var topology = client.newTopologyRequest().send().join(5, TimeUnit.SECONDS);
      final var messageSend =
          client
              .newPublishMessageCommand()
              .messageName("test")
              .correlationKey("test")
              .send()
              .join(5, TimeUnit.SECONDS);

      // then - gateway can talk to the broker
      final var proxiedPorts =
          cluster.getBrokers().values().stream()
              .map(node -> proxyRegistry.getOrCreateProxy(node.getInternalCommandAddress()))
              .map(ContainerProxy::internalPort)
              .toList();
      TopologyAssert.assertThat(topology)
          .hasClusterSize(3)
          .hasExpectedReplicasCount(1, 3)
          .hasLeaderForEachPartition(1)
          .hasBrokerSatisfying(
              b ->
                  assertThat(b.getAddress())
                      .as("broker 0 advertises the correct proxied address")
                      .isEqualTo(TOXIPROXY_NETWORK_ALIAS + ":" + proxiedPorts.get(0)))
          .hasBrokerSatisfying(
              b ->
                  assertThat(b.getAddress())
                      .as("broker 1 advertises the correct proxied address")
                      .isEqualTo(TOXIPROXY_NETWORK_ALIAS + ":" + proxiedPorts.get(1)))
          .hasBrokerSatisfying(
              b ->
                  assertThat(b.getAddress())
                      .as("broker 2 advertises the correct proxied address")
                      .isEqualTo(TOXIPROXY_NETWORK_ALIAS + ":" + proxiedPorts.get(2)));
      assertThat(messageSend.getMessageKey()).isPositive();
    }
  }

  private void configureBroker(final ZeebeBrokerNode<?> broker) {
    final var commandApiProxy = proxyRegistry.getOrCreateProxy(broker.getInternalCommandAddress());
    final var internalApiProxy = proxyRegistry.getOrCreateProxy(broker.getInternalClusterAddress());

    initialContactPoints.add(TOXIPROXY_NETWORK_ALIAS + ":" + internalApiProxy.internalPort());
    broker
        .withEnv("ZEEBE_LOG_LEVEL", "DEBUG")
        .withEnv("ATOMIX_LOG_LEVEL", "INFO")
        .withEnv("ZEEBE_BROKER_NETWORK_COMMANDAPI_ADVERTISEDHOST", TOXIPROXY_NETWORK_ALIAS)
        .withEnv(
            "ZEEBE_BROKER_NETWORK_COMMANDAPI_ADVERTISEDPORT",
            String.valueOf(commandApiProxy.internalPort()))
        .withEnv("ZEEBE_BROKER_NETWORK_INTERNALAPI_ADVERTISEDHOST", TOXIPROXY_NETWORK_ALIAS)
        .withEnv(
            "ZEEBE_BROKER_NETWORK_INTERNALAPI_ADVERTISEDPORT",
            String.valueOf(internalApiProxy.internalPort()))
        // Since gossip does not work with Toxiproxy, increase the sync interval so changes are
        // propagated faster
        .withEnv("ZEEBE_BROKER_CLUSTER_MEMBERSHIP_SYNCINTERVAL", "100ms");
  }

  private void configureGateway(final ZeebeGatewayNode<?> gateway) {
    final var gatewayClusterProxy =
        proxyRegistry.getOrCreateProxy(gateway.getInternalClusterAddress());
    final var contactPoint = cluster.getBrokers().get(0);
    final var contactPointProxy =
        proxyRegistry.getOrCreateProxy(contactPoint.getInternalClusterAddress());

    gateway
        .withEnv(
            "ZEEBE_GATEWAY_CLUSTER_CONTACTPOINT",
            TOXIPROXY_NETWORK_ALIAS + ":" + contactPointProxy.internalPort())
        .withEnv("ZEEBE_GATEWAY_CLUSTER_ADVERTISEDHOST", TOXIPROXY_NETWORK_ALIAS)
        .withEnv(
            "ZEEBE_GATEWAY_CLUSTER_ADVERTISEDPORT",
            String.valueOf(gatewayClusterProxy.internalPort()));
  }
}
