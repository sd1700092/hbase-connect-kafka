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
package io.svectors.hbase.cdc.func;

import com.google.common.base.Preconditions;
import io.svectors.hbase.cdc.model.HRow;
import io.svectors.hbase.cdc.util.DateUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author ravi.magham
 */
public class ToHRowFunction /*implements BiFunction<String, byte[], List<Cell>, HRow>*/ {


//    @Override
    public HRow apply(String hbaseTableName, byte[] rowkey, List<Cell> cells) {
        Preconditions.checkNotNull(rowkey);
        Preconditions.checkNotNull(cells);
        String rowkeyStr = Bytes.toString(rowkey);
        final List<HRow.HColumn> after = toRowColumns(rowkeyStr, cells);
        HRow.RowOp rowOp = null;
        final Cell cell = cells.get(0);
        final CellProtos.CellType type = CellProtos.CellType.valueOf(cell.getTypeByte());
        switch (type) {
            case DELETE:
            case DELETE_COLUMN:
            case DELETE_FAMILY:
                rowOp = HRow.RowOp.D;
                break;
            case PUT:
                rowOp = HRow.RowOp.S;
                break;
        }
//        final HRow row = new HRow(rowkey, rowOp, after);
        String opType = rowOp.name();
        final HRow row = new HRow(hbaseTableName, opType, DateUtils.formatDate2(new Date(System.currentTimeMillis())),DateUtils.formatDate2(new Date(System.currentTimeMillis())),"1", rowkeyStr, after);
        return row;
    }

    /**
     * maps each {@linkplain Cell} to a {@linkplain HRow.HColumn}
     * @param cells
     * @return
     */
    private List<HRow.HColumn> toRowColumns(String rowkey, final List<Cell> cells) {
        final List<HRow.HColumn> columns = cells.stream().map(cell -> {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long timestamp = cell.getTimestamp();
            final HRow.HColumn column = new HRow.HColumn(rowkey, family, qualifier, value, timestamp);
            return column;
        }).collect(toList());

        return columns;
    }
}
