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
package io.svectors.hbase.cdc.model;

import org.apache.hadoop.hbase.HConstants;

import java.util.Arrays;
import java.util.List;

/**
 *  @author ravi.magham
 */
public class HRow {

    private byte[] rowKey;

    private RowOp rowOp;

    private List<HColumn> columns;

    public HRow(){}

    public HRow(byte[] rowKey, RowOp rowOp, HColumn... columns) {
        this(rowKey, rowOp, Arrays.asList(columns));
    }

    public HRow(byte[] rowKey, RowOp rowOp, List<HColumn> columns) {
        this.rowKey = rowKey;
        this.rowOp = rowOp;
        this.columns = columns;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public RowOp getRowOp() {
        return rowOp;
    }

    public List<HColumn> getColumns() {
        return columns;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public void setRowOp(RowOp rowOp) {
        this.rowOp = rowOp;
    }

    public void setColumns(List<HColumn> columns) {
        this.columns = columns;
    }

    /**
     * Properties for a column .
     */
    public static class HColumn {

        private byte[] family;

        private long timestamp;

        private byte[] qualifier;

        private byte[] value;

        public HColumn(){}

        public HColumn(byte[] family, byte[] qualifier, byte[] value) {
            this(family,qualifier,value,HConstants.LATEST_TIMESTAMP);
        }

        public HColumn(byte[] family, byte[] qualifier, byte[] value, long timestamp) {
            this.family = family;
            this.qualifier = qualifier;
            this.value = value;
            this.timestamp = timestamp;
        }

        public byte[] getFamily() {
            return family;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public byte[] getQualifier() {
            return qualifier;
        }

        public byte[] getValue() {
            return value;
        }

        public void setFamily(byte[] family) {
            this.family = family;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public void setQualifier(byte[] qualifier) {
            this.qualifier = qualifier;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }
    }

    public static enum RowOp {
        PUT,
        DELETE;
    }
}
