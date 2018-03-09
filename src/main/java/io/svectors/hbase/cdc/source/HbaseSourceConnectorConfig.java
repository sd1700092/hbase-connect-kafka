/**
 * Created by jpbirdy on 17/5/11.
 */

package io.svectors.hbase.cdc.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * @author jialou.jp
 * @project HdfsKafkaConnector
 * @class HbaseSourceConnectorConfig
 * @date 17/5/11 23:43
 * @desc
 */
public class HbaseSourceConnectorConfig extends AbstractConfig {
  public static final String CONNECTOR_GROUP = "Connector";
  public static final String KAFKA_BOOTSTAP_SERVERS = "kafka.bootstrap.servers";
  private static final String KAFKA_BOOTSTAP_SERVERS_DOC =
      "Fill In the Kafka Servers like 172.31.217.111:9092,172.31.217.115:9092,172.31.217.124:9092";
  public static final String KAFKA_ACKS = "kafka.acks";
  public static final String KAFKA_KEY_SERIALIZER = "kafka.key.serializer";
  public final static String KAFKA_PRODUCER_TYPE = "kafka.producer.type";
  public static final String KAFKA_VALUE_SERIALIZER = "kafka.value.serializer";
  public static final ConfigDef CONFIG_DEF = baseConfigDef();

  public static ConfigDef baseConfigDef() {
    ConfigDef config = new ConfigDef();
    config.define(KAFKA_BOOTSTAP_SERVERS, ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH, KAFKA_BOOTSTAP_SERVERS_DOC, CONNECTOR_GROUP, 4,
        ConfigDef.Width.MEDIUM, "")
        .define(KAFKA_ACKS, ConfigDef.Type.STRING,
            ConfigDef.Importance.MEDIUM, "", CONNECTOR_GROUP, 4,
            ConfigDef.Width.MEDIUM, "")
        .define(KAFKA_KEY_SERIALIZER, ConfigDef.Type.STRING,
            ConfigDef.Importance.MEDIUM, "", CONNECTOR_GROUP, 4,
            ConfigDef.Width.MEDIUM, "")
            .define(KAFKA_PRODUCER_TYPE, ConfigDef.Type.STRING,
                    ConfigDef.Importance.MEDIUM, "", CONNECTOR_GROUP, 4,
                    ConfigDef.Width.MEDIUM, "")
            .define(KAFKA_VALUE_SERIALIZER, ConfigDef.Type.STRING,
            ConfigDef.Importance.MEDIUM, "", CONNECTOR_GROUP, 4,
            ConfigDef.Width.MEDIUM, "");
      return config;
  }

  private static boolean classNameEquals(String className, Class<?> clazz) {
    return className.equals(clazz.getSimpleName()) || className.equals(clazz.getCanonicalName());
  }

  public HbaseSourceConnectorConfig(Map<String, String> props) {
    super(CONFIG_DEF, props);
  }
}
