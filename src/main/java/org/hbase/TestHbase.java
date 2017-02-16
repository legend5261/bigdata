package org.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Hbase基本操作
 *
 * @YuChuanQi
 * @since 2016-09-09 17:20:20
 */
public class TestHbase {
    private static final String table = "testtable";
    private static final String zkQuorumValue = "jyibd58,jyibd57,jyibd59,jyibd56,jyibd60";

    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkQuorumValue);
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);
        //conf.set("hbase.master", "jyibd61:60000");
        //conf.set("hbase.rootdir", "hdfs://jyibd61:8020/hbase");
    }

    public static void main(String[] args) throws Exception{
        String testTable = "test_ycq";
        //createTable(testTable);
        //insertData(testTable);
        //queryAll(testTable);
        //queryAll(table);
        //queryByCondition1(testTable, "row1");
        queryByCondition2(testTable);
    }

    public static void createTable(String tableName) throws Exception {
        System.out.println("start create table ...");
        Connection connection = null;
        Admin hbaseAdmin = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            hbaseAdmin = connection.getAdmin();
            System.out.println("Connected " + conf.get("hbase.rootdir"));
            TableName tb = TableName.valueOf(tableName);
            if (hbaseAdmin.tableExists(tb)) {
                hbaseAdmin.disableTable(tb);
                hbaseAdmin.deleteTable(tb);
                System.out.println(tableName + " is exist, delete ...");
                //throw new RuntimeException("table is already exist");
            }

            HTableDescriptor tableDescriptor = new HTableDescriptor(tb);
            tableDescriptor.addFamily(new HColumnDescriptor("table1".getBytes()));
            tableDescriptor.addFamily(new HColumnDescriptor("table2".getBytes()));
            tableDescriptor.addFamily(new HColumnDescriptor("table3".getBytes()));
            hbaseAdmin.createTable(tableDescriptor);
            System.out.println("created ...");
        } finally {
            connection.close();
            hbaseAdmin.close();
        }

    }

    public static void insertData(String tableName) throws Exception {
        System.out.println("start insert data ...");
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            TableName tb = TableName.valueOf(tableName);
            Table table = connection.getTable(tb);
            Put put = new Put("row1".getBytes());
            put.addColumn("table1".getBytes(), "name".getBytes(), "Yu".getBytes());
            put.addColumn("table1".getBytes(), "age".getBytes(), "24".getBytes());
            put.addColumn("table1".getBytes(), "height".getBytes(), "170".getBytes());

            put.addColumn("table2".getBytes(), "apple".getBytes(), "mac".getBytes());
            put.addColumn("table2".getBytes(), "huawei".getBytes(), "p9".getBytes());
            put.addColumn("table2".getBytes(), "meizu".getBytes(), "mx6".getBytes());

            Put put1 = new Put("row2".getBytes());
            put1.addColumn("table1".getBytes(), "name".getBytes(), "Gu".getBytes());
            put1.addColumn("table1".getBytes(), "age".getBytes(), "24".getBytes());
            put1.addColumn("table1".getBytes(), "height".getBytes(), "150".getBytes());
            List<Put> list = new ArrayList<Put>();
            list.add(put);
            list.add(put1);
            table.put(list);
            table.close();
            System.out.println("end insert data ...");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void dropTable(String tableName) throws Exception {
        Connection connection = null;
        Admin hbaseAdmin = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            hbaseAdmin = connection.getAdmin();
            TableName tb = TableName.valueOf(tableName);
            hbaseAdmin.disableTable(tb);
            hbaseAdmin.deleteTable(tb);
        } finally {
            connection.close();
            hbaseAdmin.close();
        }
    }

    public static void deleteRow(String tableName, String rowKey) throws Exception {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            TableName tb = TableName.valueOf(tableName);
            Table table = connection.getTable(tb);
            table.delete(new Delete(rowKey.getBytes()));
            System.out.println("删除rowkey " + rowKey + "成功");
        } finally {
            connection.close();
        }
    }

    public static void queryAll(String tableName) throws Exception {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            TableName tb = TableName.valueOf(tableName);
            Table table = connection.getTable(tb);
            ResultScanner resultScanner = table.getScanner(new Scan());
            for(Result rs : resultScanner) {
                System.out.println("RowKey : " + new String(rs.getRow(), "UTF-8"));
                for(Cell cell : rs.rawCells()) {
                    String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String qua = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("cellFamily : " + family + " qualifier : " + qua + " value : " + value);
                }
            }
        } finally {
            connection.close();
        }
    }

    /**
     * 根据rowKey查询
     */
    public static void queryByCondition1(String tableName, String rowKey) throws Exception{
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName.getBytes()));
            Result result = table.get(new Get(rowKey.getBytes()));
            System.out.println("rowKey : " + new String(result.getRow(), "UTF-8"));
            for (Cell cell : result.rawCells()) {
                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String qua = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("cellFamily : " + family + " qualifier : " + qua + " value : " + value);
            }
        } finally {
            connection.close();
        }
    }

    public static void queryByCondition2(String tableName) throws Exception {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName.getBytes()));
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("table1"), null, CompareFilter.CompareOp.EQUAL, Bytes.toBytes("Yu"));
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);
            for(Result rs : resultScanner) {
                System.out.println("RowKey : " + new String(rs.getRow(), "UTF-8"));
                for(Cell cell : rs.rawCells()) {
                    String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String qua = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("cellFamily:" + family + " |qualifier:" + qua + " |value:" + value);
                }
            }
        } finally {
            connection.close();
        }
    }
}
