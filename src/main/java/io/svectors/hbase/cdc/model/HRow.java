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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *  @author ravi.magham
 */
public class HRow {

    private String collection;

    private String op_type;

    private String op_ts;

    private String current_ts;

    private String pos;

    private List<String> primary_keys = new ArrayList<>();

    private byte[] rowKey;

    private RowOp rowOp;

    private List<HColumn> after;

    public HRow(){}

    public HRow(byte[] rowKey, RowOp rowOp, HColumn... after) {
        this(rowKey, rowOp, Arrays.asList(after));
    }

    public HRow(byte[] rowKey, RowOp rowOp, List<HColumn> after) {
        this.rowKey = rowKey;
        this.rowOp = rowOp;
        this.after = after;
    }

    public HRow(String collection, String op_type, String op_ts, String current_ts, String pos, String primary_keys,  List<HColumn> after) {
        this.collection = collection;
        this.op_type = op_type;
        this.op_ts = op_ts;
        this.current_ts = current_ts;
        this.pos = pos;
        this.primary_keys.add(primary_keys) ;
        this.after = after;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public RowOp getRowOp() {
        return rowOp;
    }

    public void setRowOp(RowOp rowOp) {
        this.rowOp = rowOp;
    }

    public List<HColumn> getAfter() {
        return after;
    }

    public void setAfter(List<HColumn> after) {
        this.after = after;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getOp_type() {
        return op_type;
    }

    public void setOp_type(String op_type) {
        this.op_type = op_type;
    }

    public String getOp_ts() {
        return op_ts;
    }

    public void setOp_ts(String op_ts) {
        this.op_ts = op_ts;
    }

    public String getCurrent_ts() {
        return current_ts;
    }

    public void setCurrent_ts(String current_ts) {
        this.current_ts = current_ts;
    }

    public String getPos() {
        return pos;
    }

    public void setPos(String pos) {
        this.pos = pos;
    }

    public List<String> getPrimary_keys() {
        return primary_keys;
    }

    public void setPrimary_keys(List<String> primary_keys) {
        this.primary_keys = primary_keys;
    }

    /**
     * Properties for a column .
     */
    public static class HColumn {
        private String rowkey;

        private String family;

        private long timestamp;

        private String qualifier;

        private String value;

        public HColumn() {
        }

        public HColumn(String rowkey, String family, String qualifier, String value) {
            this(rowkey, family, qualifier, value, HConstants.LATEST_TIMESTAMP);
        }

        public HColumn(String rowkey, String family, String qualifier, String value, long timestamp) {
            this.rowkey = rowkey;
            this.family = family;
            this.qualifier = qualifier;
            this.value = value;
            this.timestamp = timestamp;
        }

        public String getRowkey() {
            return rowkey;
        }

        public void setRowkey(String rowkey) {
            this.rowkey = rowkey;
        }

        public String getFamily() {
            return family;
        }

        public void setFamily(String family) {
            this.family = family;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getQualifier() {
            return qualifier;
        }

        public void setQualifier(String qualifier) {
            this.qualifier = qualifier;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }


    public static enum RowOp {
        S,       // 代表HBASE的PUT操作
        D;       // 代表HBASE的DELETE操作
    }
}
