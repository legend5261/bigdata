package org.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HbaseJob {
    static Configuration conf=null;
    private static final String zkQuorumValue = "uhadoop-sasldd-master1,uhadoop-sasldd-master2,uhadoop-sasldd-core1";
    static{
        conf= HBaseConfiguration.create();//hbase的配置信息
        conf.set("hbase.zookeeper.quorum", zkQuorumValue);
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    }
    public static void main(String[] args)throws Exception {
        HbaseJob t=new HbaseJob();
        //t.createTable("person", new String[]{"name","age"});
        //t.insertRow("person", "1", "age", "hehe", "100");
        //t.insertRow("person", "2", "age", "haha", "101");
        //t.showAll("person");
        t.queryByCondition1("person", "3");
        DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date1 = simpleDateFormat.parse("2017-03-29 12:00:00");
        Date date2 = simpleDateFormat.parse("2017-03-29 18:00:00");


        t.deleteTimeRange("person", date1.getTime(), date2.getTime());
    }

    /**
     * 根据时间戳批量删除
     * @param tableName
     * @param minTime
     * @param maxTime
     * @throws Exception
     */
    public void deleteTimeRange(String tableName, long minTime, long maxTime) throws Exception {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName.getBytes()));
            Scan scan = new Scan();
            scan.setTimeRange(minTime, maxTime);
            ResultScanner rs = table.getScanner(scan);
            List<Delete> list = new ArrayList<Delete>();
            for (Result r : rs) {
                Delete delete = new Delete(r.getRow());
                list.add(delete);

                System.out.println("RowKey : " + new String(r.getRow(), "UTF-8"));
                for(Cell cell : r.rawCells()) {
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

    public void showAll(String tableName)throws Exception {
        HTable h=new HTable(conf, tableName);
        Scan scan=new Scan();
        ResultScanner scanner=h.getScanner(scan);
        for(Result r:scanner){
            System.out.println("====");
            for(KeyValue k:r.raw()){
                System.out.println("行号:  "+ Bytes.toStringBinary(k.getRow()));
                System.out.println("时间戳:  "+k.getTimestamp());
                System.out.println("列簇:  "+ Bytes.toStringBinary(k.getFamily()));
                System.out.println("列:  "+ Bytes.toStringBinary(k.getQualifier()));
                String ss=  Bytes.toString(k.getValue());
                System.out.println("值:  "+ss);
            }
        }
        h.close();
    }

    public void queryByCondition1(String tableName, String rowKey) throws Exception {
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

    /***
     * 创建一张表
     * 并指定列簇
     * */
    public void createTable(String tableName, String cols[])throws Exception {
        HBaseAdmin admin=new HBaseAdmin(conf);//客户端管理工具类
        if(admin.tableExists(tableName)){
            System.out.println("此表已经存在.......");
        }else{
            HTableDescriptor table=new HTableDescriptor(tableName);
            for(String c:cols){
                HColumnDescriptor col=new HColumnDescriptor(c);//列簇名
                table.addFamily(col);//添加到此表中
            }
            admin.createTable(table);//创建一个表
            admin.close();
            System.out.println("创建表成功!");
        }
    }

    public  void insertRow(String tableName, String row,
                           String columnFamily, String column, String value) throws Exception {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
        table.close();//关闭
        System.out.println("插入一条数据成功!");
    }


}
