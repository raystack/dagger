package com.gojek.daggers.config;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Base64;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationsTest {

  private ConfigurationProviderFactory providerFactory;

  @Before
  public void setUp() {
    System.setProperty("key", "envValue");
    String[] args = {Base64.getEncoder().encodeToString("[\"--key\", \"argValue\"]".getBytes())};
    providerFactory = new ConfigurationProviderFactory(args);
  }

  @Ignore
  @Test
  public void shouldProvideFromEnvironmentBasedOnConfigProperty() {
    System.setProperty("ConfigSource", "ENVIRONMENT");

    ConfigurationProvider provider = providerFactory.provider();

    assertEquals(provider.get().getString("key", ""), "envValue");

  }

  @Test
  public void shouldProvideFromCommandline() {
    System.setProperty("ConfigSource", "ARGS");

    ConfigurationProvider provider = providerFactory.provider();

    assertEquals(provider.get().getString("key", ""), "argValue");
  }
}
