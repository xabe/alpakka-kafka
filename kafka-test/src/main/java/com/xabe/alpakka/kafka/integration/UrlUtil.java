package com.xabe.alpakka.kafka.integration;

import static java.lang.String.format;

public class UrlUtil {

  private static final String SCHEMA_REGISTRY = "http://%s:%s/subjects/%s-value/versions";

  private static final String SCHEMA_REGISTRY_COMPATIBILITY = "http://%s:%s/config/%s-value";

  private static final UrlUtil INSTANCE = new UrlUtil();

  private final String urlSchemaRegistry;

  private final String urlSchemaRegistryCompatibility;

  private UrlUtil() {
    final String registryHost = System.getProperty("schemaregistry.host", "localhost");
    final String registryPort = System.getProperty("schemaregistry.port", "8081");
    this.urlSchemaRegistry = format(SCHEMA_REGISTRY, registryHost, registryPort, "cars.v1");
    this.urlSchemaRegistryCompatibility = format(SCHEMA_REGISTRY_COMPATIBILITY, registryHost, registryPort, "cars.v1");
  }

  public static UrlUtil getInstance() {
    return INSTANCE;
  }

  public String getUrlSchemaRegistry() {
    return this.urlSchemaRegistry;
  }

  public String getUrlSchemaRegistryCompatibility() {
    return this.urlSchemaRegistryCompatibility;
  }
}
