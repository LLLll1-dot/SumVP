package GraphSummarization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 实现hbase的操作，如：初始化hbase连接、建表、根据key返回对应的value、根据key删除对应的记录
 *
 * @author yzp
 *
 */
public class HBaseUtil {

    private static Configuration conf = HBaseConfiguration.create();
    private static Connection coon = null;
    private static Admin admin = null;

    /**
     * 测试
     * @param args
     */
    public static void main(String[] args) {

        init();
        String[] columns = { "c1" };
        createTable("Try.txt", columns);
    }

    /**
     * 初始化hbase连接
     */
    public static void init() {
        conf.set("hbase.zookeeper.quorum", "master,worker1,worker2,worker3,worker4");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            coon = ConnectionFactory.createConnection(conf);
            admin = coon.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据传入的表名和列族数组创建表，如果表已经存在，会将存在的表删除并新建。
     * @param tableName
     * @param columnfamily
     */
    public static void createTable(String tableName, String[] columnfamily) {
        createOrOverWriteATable(TableName.valueOf(tableName), columnfamily);
    }

    private static void createOrOverWriteATable(TableName tableName,
                                                String[] families) {
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            HTableDescriptor htd = new HTableDescriptor(tableName);
            for (String family : families) {
                htd.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(htd);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void updateByKey(String key,String value){
        updateByKey("Enron", "idFam", "idsList", key, value);
    }

    private static void updateByKey(String tablename, String columnFam,
                                    String qualifier, String key, String value) {

        String originStr,newVal;
        Table table;
        Get g;
        Put p;

        try {
            table = coon.getTable(TableName.valueOf(tablename));

            g = new Get(key.getBytes());//确定行
            g.addColumn(columnFam.getBytes(), qualifier.getBytes());//确定列族和相关限定符
            Result r = table.get(g);

            byte[] b = r.value();
            originStr = Bytes.toString(b);

//			if (originStr.indexOf(value) < 0) {
//				newVal = originStr + "\t" + value;
            newVal = value;
            p = new Put(key.getBytes());
            p.addColumn(columnFam.getBytes(), qualifier.getBytes(), newVal.getBytes());
            table.put(p);
//			}

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(){
        if (coon != null) {
            try {
                coon.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从NodeMap表中获取对应的节点信息
     * @param key
     * @return
     */
    public static String getNodeInfoByKey(String key){

        return getValueByKey("Enron", "idFam", "idsList", key);

    }
    private static String getValueByKey(String tableName, String columnFam,
                                        String qualifier, String columnStr) {
        //初始化连接
        String resStr = "";
        Table table = null;
        Get g = null;
        try {
            table = coon.getTable(TableName.valueOf(tableName));//确定表
            g = new Get(columnStr.getBytes());//确定行
            g.addColumn(columnFam.getBytes(), qualifier.getBytes());//确定列族和相关限定符
            Result r = table.get(g);

            byte[] b = r.value();
//			resStr = b.toString();
            resStr = Bytes.toString(b);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return resStr;
    }
}

