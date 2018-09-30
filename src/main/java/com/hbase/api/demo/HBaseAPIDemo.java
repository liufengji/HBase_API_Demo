package com.hbase.api.demo;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseAPIDemo {

    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
    }

    /**
     * 判断HBase表是否存在
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean isExisTables(String tableName) throws Exception {
        // 操作HBase表必须创建HBaseAdmin对象
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        return hAdmin.tableExists(tableName);
    }

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     * @throws Exception
     */
    private static void createTable(String tableName, String... columnFamily) throws Exception {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        // 判断表是否存在
        if (isExisTables(tableName)) {
            // 存在
            System.out.println("表已经存在" + tableName);
            System.exit(0);
        } else {
            // 不存在
            // 通过表名实例化"表描述器"
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                tableDescriptor.addFamily(new HColumnDescriptor(cf));
            }

            hAdmin.createTable(tableDescriptor);
            System.out.println("表创建成功" + tableName);
        }

    }

    /**
     * 删除表
     * @param tableName
     * @throws Exception
     */
    private static void dropTable(String tableName) throws Exception {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (isExisTables(tableName)) {
            // 存在
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println("删除表成功" + tableName);
        } else {
            // 不存在
            System.out.println("表不存在" + tableName);
        }
    }


    /**
     * 添加一行数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @param Value
     * @throws Exception
     */
    private static void addRow(String tableName, String rowKey, String cf, String cn, String Value) throws Exception {
        HTable hTable = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(Value));
        hTable.put(put);
        hTable.close();
        System.out.println("添加数据成功");
    }


    //删除一行
    public static void deleteRow(String tableName, String rowKey) throws Exception {
        HTable hTable = new HTable(conf, tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        hTable.delete(delete);
        hTable.close();
        System.out.println("删除数据成功");
    }

    /**
     * 删除多行
     *
     * @param tableName
     * @param rowKeys
     * @throws Exception
     */
    public static void deleteMultiRows(String tableName, String... rowKeys) throws Exception {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        System.out.println("多行删除成功");
    }

    /**
     * 获取所有数据
     * @param tableName
     * @throws Exception
     */
    public static void getAllRows(String tableName) throws Exception {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell c : cells) {
                System.out.println("行:" + Bytes.toString(CellUtil.cloneRow(c)));
                System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(c)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(c)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(c)));
            }
        }

    }

    /**
     * 获取一行
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public static void getRow(String tableName,String rowKey) throws Exception{
        HTable hTable = new HTable(conf,tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();
        for (Cell c : cells) {
            System.out.println("行:" + Bytes.toString(CellUtil.cloneRow(c)));
            System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(c)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(c)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(c)));
        }
    }

    /**
     * 只获取一列
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @throws Exception
     */
    public static void getRowQualifier(String tableName,String rowKey,String cf,String cn) throws Exception{

        HTable hTable = new HTable(conf,tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(cf)).addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        Result result = hTable.get(get);
        Cell[] cells = result.rawCells();

        for (Cell c : cells) {
            System.out.println("行:" + Bytes.toString(CellUtil.cloneRow(c)));
            System.out.println("列族:" + Bytes.toString(CellUtil.cloneFamily(c)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(c)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(c)));
        }
    }

    public static void main(String[] args) throws Exception {

        //判断HBase表是否存在
        //System.out.println(isExisTables("Student"));

        //创建表
        // createTable("staff", "info","fa");

        //删除表
        // dropTable("staff");

        //添加一行数据
        //addRow("staff", "1001", "info", "name", "Nick");
        //addRow("staff", "1002", "info", "name", "Nick");
        //addRow("staff", "1003", "info", "sex", "女");

        //删除一行
        // deleteRow("staff", "1001");

        //删除多行
        // deleteMultiRows("staff","1001","1002","1003");

        //获取所有数据
        //getAllRows("staff");

        //获取一行
        //getRow("staff","1001");

        //只获取一列
        getRowQualifier("staff","1003","info","sex");
    }
}
