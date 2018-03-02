/**
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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @author ravi.magham
 */
public class TestKafkaReplication extends BaseTest {

    public static final Logger LOG = LoggerFactory.getLogger(TestKafkaReplication.class);

    private static final String PEER_NAME = "hbase.cdc.kafka";
		protected final TableName TABLE_NAME = TableName.valueOf("testings");
		protected final String ROWKEY = "rk-%s";
		protected final String COLUMN_FAMILY = "d";
		protected final String QUALIFIER = "q";
		protected final String VALUE = "v";

		@Test
    public void testCustomReplicationEndpoint() throws Exception {
				try {
	          Map<TableName, List<String>> tableCfs = new HashMap<>();
	          List<String> cfs = new ArrayList<>();
	          cfs.add(COLUMN_FAMILY);
	          tableCfs.put(TABLE_NAME, cfs);

	          createTestTable();
	          addPeer(utility.getConfiguration(), PEER_NAME, tableCfs);
						int numberOfRecords = 10;
	          addData(numberOfRecords);

	          final KafkaConsumer kafkaConsumer = createAndGetKafkaConsumer();
		        final AtomicInteger totalRecords = new AtomicInteger(0);
	          kafkaConsumer.subscribe(Collections.singletonList(TABLE_NAME.getNameAsString()));
		        while (totalRecords.get() < numberOfRecords) {
			          ConsumerRecords<byte[], String> consumerRecords = kafkaConsumer.poll(1000);
			          if(consumerRecords != null && !consumerRecords.isEmpty()) {
						  System.out.println("Received messages");
						  consumerRecords.forEach(record -> {
					              final String expectedRowkey = String.format(ROWKEY, totalRecords.getAndAdd(1));
								System.out.println(expectedRowkey + ", " + record.value()/*.getRowOp()*/);
//								Assert.assertEquals(expectedRowkey, Bytes.toString(record.value().getRowKey()));
				            });
				        } else {
						  System.out.println("ConsumerRecords is null!");
					  }
		        }
		        kafkaConsumer.close();
        } finally {
	          removePeer();
        }
	  }

    /**
     * Create the hbase table with a scope set to Global
     * @throws IOException
     */
    private void createTestTable() throws IOException {
        try(HBaseAdmin hBaseAdmin = utility.getHBaseAdmin()) {
            final HTableDescriptor hTableDescriptor = new HTableDescriptor(TABLE_NAME);
            final HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(COLUMN_FAMILY);
            hColumnDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
            hTableDescriptor.addFamily(hColumnDescriptor);
            hBaseAdmin.createTable(hTableDescriptor);
        }
        utility.waitUntilAllRegionsAssigned(TABLE_NAME);
    }

    /**
     * Adds data to the previously created HBase table
     * @throws IOException
     */
    private void addData(int numberOfRecords) throws IOException {
        try(Table hTable = ConnectionFactory.createConnection(utility.getConfiguration()).getTable(TABLE_NAME)) {
            for(int i = 0; i < numberOfRecords; i++) {
	              Put put = new Put(toBytes(String.format(ROWKEY, i)));
	              put.addColumn(toBytes(COLUMN_FAMILY), toBytes(QUALIFIER), toBytes(VALUE));
	              hTable.put(put);
				System.out.println("data added");
			}
        }
    }

    /**
     * Removes the peer
     * @throws IOException
     * @throws ReplicationException
     */
    private void removePeer() throws IOException, ReplicationException {
        try(ReplicationAdmin replicationAdmin = new ReplicationAdmin(utility.getConfiguration())) {
            replicationAdmin.removePeer(PEER_NAME);
        }
    }
}
