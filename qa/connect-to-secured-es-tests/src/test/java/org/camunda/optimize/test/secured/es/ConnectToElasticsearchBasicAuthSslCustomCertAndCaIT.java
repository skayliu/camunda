/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.test.secured.es;

import org.camunda.optimize.IgnoreDuringScan;
import org.camunda.optimize.service.util.configuration.ConfigurationService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import static org.camunda.optimize.service.util.configuration.ConfigurationServiceBuilder.createConfigurationWithDefaultAndAdditionalLocations;

@Import(org.camunda.optimize.Main.class)
@IgnoreDuringScan
public class ConnectToElasticsearchBasicAuthSslCustomCertAndCaIT extends AbstractConnectToElasticsearchIT {

  private static final String CONFIG_FILE = "secured-connection-basic-auth-ssl-custom-cert-and-ca.yaml";

  @Override
  protected String getCustomConfigFile() {
    return CONFIG_FILE;
  }

  @TestConfiguration
  static class Configuration {
    @Bean
    @Primary
    public ConfigurationService configurationService() {
      return createConfigurationWithDefaultAndAdditionalLocations(CONFIG_FILE);
    }
  }
}
