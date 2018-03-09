package io.svectors.hbase.cdc.hbasesource;

import com.alibaba.fastjson.JSON;
import io.svectors.hbase.cdc.model.HRow;
import io.svectors.hbase.cdc.source.HbaseSourceConnectorConfig;
import io.svectors.hbase.cdc.util.TopicNameFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.svectors.hbase.cdc.HbaseEndpoint.TO_HROW;
import static java.util.stream.Collectors.groupingBy;


/**
 * @author jialou.jp
 * @project HdfsKafkaConnector
 * @class HBaseSourceTask
 * @date 17/5/11 21:05
 * @desc
 */
public class HBaseSourceTask extends SourceTask {
    public final static String TARGET = "TARGET";
    private static final Logger log = LoggerFactory.getLogger(HBaseSourceTask.class);
    private static final int READ_SLEEP = 5000;
    private static final int BUFFER_SIZE = 4096;
    //  读取hdfs,控制一个文件单次最大消息量
    private static final int FILE_MAX_MESSAGE_SIZE = 4096;
    private String kafkaKeySerializer = "";
    private String kafkaValueSerilizer = "";
    private String kafkaBootStapServers = "";
    private String kafkaProducerType = "";
    private String kafkaAcks = "";
    private ReplicationEndpoint.ReplicateContext ctx;
    private TopicNameFilter topicNameFilter;
    private Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>(0);
    private String target = "";
    private String hdfsUrl = "";
    private String type = "";
    private Configuration conf = new Configuration();
    private Map<String, Long> fileOffset = new HashMap<>();
    private volatile boolean isRunning;

    public void setCtx(ReplicationEndpoint.ReplicateContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    @Override
    public void start(Map<String, String> props) {
        System.out.println("Source TASK start: props is ");
        System.out.println(props);

        HbaseSourceConnectorConfig connectorConfig = new HbaseSourceConnectorConfig(props);
        kafkaKeySerializer = props.get(HbaseSourceConnectorConfig.KAFKA_KEY_SERIALIZER);
        kafkaValueSerilizer = props.get(HbaseSourceConnectorConfig.KAFKA_VALUE_SERIALIZER);
        kafkaBootStapServers = props.get(HbaseSourceConnectorConfig.KAFKA_BOOTSTAP_SERVERS);
        kafkaProducerType = props.get(HbaseSourceConnectorConfig.KAFKA_PRODUCER_TYPE);
        kafkaAcks = props.get(HbaseSourceConnectorConfig.KAFKA_ACKS);

    }

    private String generateTopic(String file) {
        String suffix = file.replaceAll("/", "_");
//    return topicPrefix + suffix;
        String topicPrefix = "";
        return topicPrefix;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<Map<String, String>> partitions = new ArrayList<>();
        offsets.putAll(context.offsetStorageReader().offsets(partitions));

        ArrayList<SourceRecord> records = new ArrayList<>();
        final List<WAL.Entry> entries = ctx.getEntries();
        try {
            final Map<String, List<WAL.Entry>> entriesByTable = entries.stream()
                    .filter(entry -> topicNameFilter.test(entry.getKey().getTablename().getNameAsString()))
                    .collect(groupingBy(entry -> entry.getKey().getTablename().getNameAsString()));

            // persist the data to kafka in parallel.
            entriesByTable.entrySet().forEach(entry -> {
                final String tableName = entry.getKey();
                final List<WAL.Entry> tableEntries = entry.getValue();

                tableEntries.forEach(tblEntry -> {
                    List<Cell> cells = tblEntry.getEdit().getCells();

                    // group the data by the rowkey.
                    Map<byte[], List<Cell>> columnsByRow = cells.stream()
                            .collect(groupingBy(CellUtil::cloneRow));

                    // build the list of rows.
                    columnsByRow.entrySet().forEach(rowcols -> {
                        final byte[] rowkey = rowcols.getKey();
                        final List<Cell> columns = rowcols.getValue();
                        final HRow row = TO_HROW.apply(tableName, rowkey, columns);
                        System.out.println("row: " + row);
//                        producer.send(tableName, row);
                        records.add(new SourceRecord(Collections.singletonMap("hbaseTable", row.getCollection()), Collections.singletonMap("hbaseOffset", row.getCurrent_ts()), row.getCollection(), Schema.BYTES_SCHEMA, row.getRowKey(), Schema.STRING_SCHEMA, JSON.toJSONString(row)));
                    });
                });
            });

            return records;
        } catch (Exception e) {
            // Underlying stream was killed, probably as a result of calling stop.
            // Allow to return null, and driving thread will handle any shutdown if necessary.
        }

        return null;
    }

      @Override
    public void stop() {
        try {
//      fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setTopicNameFilter(TopicNameFilter topicNameFilter) {
        this.topicNameFilter = topicNameFilter;
    }
}
