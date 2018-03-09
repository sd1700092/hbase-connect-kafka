package io.svectors.hbase.cdc.hbasesource;

import io.svectors.hbase.cdc.source.HbaseSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.svectors.hbase.cdc.source.HbaseSourceConnectorConfig.*;

/**
 * @author jialou.jp
 * @project HdfsKafkaConnector
 * @class HBaseSourceConnector
 * @date 17/5/11 21:15
 * @desc
 */
public class HBaseSourceConnector extends SourceConnector {
  public String kafkaKeySerializer = "";
  public String kafkaValueSerilizer = "";
  public String connectGroup = "";
  public String kafkaBootStapServers = "";
  public String kafkaProducerType = "";
  public String kafkaAcks = "";

  private List<String> files = new ArrayList<>();
  private List<String> paths = new ArrayList<>();

  private Map<String, String> configProperties;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }


  @Override
  public void start(Map<String, String> props) {
    System.out.println(props);
    configProperties = props;
    if (!props.containsKey(HbaseSourceConnectorConfig.KAFKA_KEY_SERIALIZER)) {
      throw new ConnectException(HbaseSourceConnectorConfig.KAFKA_KEY_SERIALIZER);
    }

    if (!props.containsKey(CONNECTOR_GROUP)) {
      throw new ConnectException(CONNECTOR_GROUP);
    }

    if (!props.containsKey(KAFKA_BOOTSTAP_SERVERS)) {
      throw new ConnectException(KAFKA_BOOTSTAP_SERVERS);
    }

    if (!props.containsKey(HbaseSourceConnectorConfig.KAFKA_PRODUCER_TYPE)) {
      throw new ConnectException(HbaseSourceConnectorConfig.KAFKA_PRODUCER_TYPE);
    }

    if (!props.containsKey(HbaseSourceConnectorConfig.KAFKA_ACKS)) {
      throw new ConnectException(HbaseSourceConnectorConfig.KAFKA_ACKS);
    }

    if (!props.containsKey(HbaseSourceConnectorConfig.KAFKA_VALUE_SERIALIZER)) {
      throw new ConnectException(HbaseSourceConnectorConfig.KAFKA_VALUE_SERIALIZER);
    }

    kafkaKeySerializer = props.get(HbaseSourceConnectorConfig.KAFKA_KEY_SERIALIZER);
    kafkaValueSerilizer = props.get(HbaseSourceConnectorConfig.KAFKA_VALUE_SERIALIZER);
    kafkaAcks = props.get(HbaseSourceConnectorConfig.KAFKA_ACKS);
    kafkaProducerType = props.get(HbaseSourceConnectorConfig.KAFKA_PRODUCER_TYPE);
    kafkaBootStapServers = props.get(KAFKA_BOOTSTAP_SERVERS);
    connectGroup = props.get(CONNECTOR_GROUP);

  }

  @Override
  public Class<? extends Task> taskClass() {
    return HBaseSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> config = new HashMap<>();
    config.put(CONNECTOR_GROUP, connectGroup);
    config.put(KAFKA_BOOTSTAP_SERVERS, kafkaBootStapServers);
    config.put(KAFKA_KEY_SERIALIZER, kafkaKeySerializer);
    config.put(KAFKA_VALUE_SERIALIZER, kafkaValueSerilizer);
    config.put(KAFKA_PRODUCER_TYPE, kafkaProducerType);
    config.put(KAFKA_ACKS, kafkaAcks);
    configs.add(config);
    return configs;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return HbaseSourceConnectorConfig.CONFIG_DEF;
  }
  
}
