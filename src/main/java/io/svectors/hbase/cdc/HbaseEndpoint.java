/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.svectors.hbase.cdc;

import io.svectors.hbase.cdc.config.KafkaConfiguration;
import io.svectors.hbase.cdc.func.ToHRowFunction;
import io.svectors.hbase.cdc.hbasesource.HBaseSourceConnector;
import io.svectors.hbase.cdc.hbasesource.HBaseSourceTask;
import io.svectors.hbase.cdc.util.TopicNameFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *  @author ravi.magham
 */
public class HbaseEndpoint extends BaseReplicationEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseEndpoint.class);

    public static final ToHRowFunction TO_HROW = new ToHRowFunction();
    private KafkaMessageProducer producer;
    private TopicNameFilter topicNameFilter;

    public HbaseEndpoint() {
        super();
    }

    @Override
    public void init(Context context) throws IOException {
        super.init(context);
		LOG.info("HbaseEndpoint init: ");
    }

    @Override
    public UUID getPeerUUID() {
        return UUID.randomUUID();
    }

    /**
     *
     * @param context
     * @return
     */
    @Override
    public boolean replicate(ReplicateContext context) {
        HBaseSourceConnector hBaseSourceConnector = new HBaseSourceConnector();
        Map<String, String> props = new HashMap<>();
        //TODO: props.put(...)
        hBaseSourceConnector.start(props);

        HBaseSourceTask hBaseSourceTask = new HBaseSourceTask();

        hBaseSourceTask.setCtx(context);
        hBaseSourceTask.setTopicNameFilter(topicNameFilter);

        return true;
    }

    @Override
    protected void doStart() {
        LOG.info("Hbase replication to Kafka started at " + LocalDate.now());
        final Configuration hdfsConfig = ctx.getConfiguration();
        final KafkaConfiguration kafkaConfig = new KafkaConfiguration(hdfsConfig);
	    topicNameFilter = new TopicNameFilter(kafkaConfig);
        producer = KafkaProducerFactory.getInstance(kafkaConfig);
        notifyStarted();
    }

    @Override
    protected void doStop() {
        LOG.info("Hbase replication to Kafka stopped at " + LocalDate.now());
	    producer.close();
        notifyStopped();
    }
}
