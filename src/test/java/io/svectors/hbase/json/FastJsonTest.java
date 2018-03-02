package io.svectors.hbase.json;

import com.alibaba.fastjson.JSON;
import io.svectors.hbase.cdc.model.HRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * @author Administrator
 * created: 2018-03-01 10:48
 */
public class FastJsonTest {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.31.217.143,172.31.217.144,172.31.217.145");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "hdfs://bd-dev-hadoop2:8020/hbase");
        try {
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            TableName tn = TableName.valueOf("test");
            if (admin.tableExists(tn)) {
                System.out.println("table exists");
            } else {
                System.out.println("table not exists");
            }
            Get get = new Get(Bytes.toBytes("r1"));
            get.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c2"));
            Table table = conn.getTable(tn);
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System. out .println(
                        "Rowkey : " +Bytes. toString (result.getRow())+
                                "   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier (cell))+
                                "   Value : " +Bytes. toString (CellUtil. cloneValue (cell))+
                                "   Time : " +cell.getTimestamp()
                );
            }
            System.out.println("=========================================================================================================================");
            List<HRow.HColumn> columns = cells.stream().map(cell -> {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                long timestamp = cell.getTimestamp();
                System. out .println(
                        "Rowkey : " +Bytes. toString (result.getRow())+
                                "   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier (cell))+
                                "   Value : " +Bytes. toString (CellUtil. cloneValue (cell))+
                                "   Time : " +cell.getTimestamp()
                );
                final HRow.HColumn column = new HRow.HColumn(family, qualifier, value, timestamp);
                return column;
            }).collect(toList());
            HRow row = new HRow(Bytes.toBytes("r1"), HRow.RowOp.PUT, columns);
            String objectToJSON = JSON.toJSONString(row);
            System.out.println(objectToJSON);
            row = JSON.parseObject(objectToJSON, HRow.class);
            System.out.println(Bytes.toString(row.getRowKey()));
            List<HRow.HColumn> columnsList = row.getColumns();
            for (HRow.HColumn hColumn : columnsList) {
                System.out.println(Bytes.toString(hColumn.getFamily()) + ", " + Bytes.toString(hColumn.getQualifier()) + ", " + hColumn.getTimestamp() + ", " + Bytes.toString(hColumn.getValue()));
            }
            admin.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
