import com.sun.xml.internal.ws.transport.http.HttpAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;//会报错？？

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.text.html.HTMLDocument;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTest {
    private static Configuration conf;
    static {
        conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "h0,h1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    private static void createTable(String tableName, String[] columnfamily) {
        try {//注意必须使用try 否则报错
            Connection connection= ConnectionFactory.createConnection(conf);
            HBaseAdmin admin=(HBaseAdmin)connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                System.out.println("table"+tableName+"already exists!");
            }else{
                //创建一个描述器
                HTableDescriptor htd=new HTableDescriptor(TableName.valueOf(tableName));
                //创建列族
                for(String cf:columnfamily){
                    htd.addFamily(new HColumnDescriptor(cf));
                }
                //创建表
                admin.createTable(htd);
                System.out.println("successfully created table");
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void addRow(String tableName,String rowkey,String cf,String column,String value){
        try{
            //需要使用admin
            Connection connection = ConnectionFactory.createConnection(conf);
            Table t=connection.getTable(TableName.valueOf(tableName));
            HBaseAdmin admin=(HBaseAdmin)connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))){
                System.out.println("table dos not exist!");
            }else{
                //使用put添加数据
                Put p=new Put(Bytes.toBytes((rowkey)));
                //添加数据
                p.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
                t.put(p);
            }


        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
            throws IOException {
        System.out.println("start");
        String[] cfamily={"Description","Courses","Home"};
        createTable("student",cfamily);
        //deleteTable("tony");
        addRow("student","001","Description","Name","Li Lei");
        addRow("student","002","Description","Name","Han Meimei");
        addRow("student","003","Description","Name","Xiao Ming");
        addRow("student","001","Description","Height","176");
        addRow("student","002","Description","Height","183");
        addRow("student","003","Description","Height","162");
        addRow("student","001","Courses","Chinese","80");
        addRow("student","002","Courses","Chinese","88");
        addRow("student","003","Courses","Chinese","90");
        addRow("student","001","Courses","Math","90");
        addRow("student","002","Courses","Math","77");
        addRow("student","003","Courses","Math","80");
        addRow("student","001","Home","Province","Zhejiang");
        addRow("student","002","Home","Province","Beijing");
        addRow("student","001","Home","Province","Shanghai");
        System.out.println("finish");

    }
}
