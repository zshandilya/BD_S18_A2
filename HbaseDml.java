package dml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDml {
	
	void insert(String name, String age, String dept) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf,"company");
		Put row = new Put(Bytes.toBytes(name));
		row.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes(age));
		row.add(Bytes.toBytes("details"), Bytes.toBytes("deptt"), Bytes.toBytes(dept));
		System.out.println("Inserting record "+name+" to table company ...");
		table.put(row);
		table.close();
	}
	
	@SuppressWarnings("deprecation")
	void getRecord(String name) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf,"company");
				
		Get row = new Get(Bytes.toBytes(name));
		Result rs=table.get(row);
		
		for(KeyValue kv : rs.raw()) {
			System.out.println("Name: "+new String(kv.getRow())+" ");
			System.out.print(new String(kv.getFamily())+":");
			System.out.print(new String(kv.getQualifier())+" --> ");
			System.out.println(new String(kv.getValue()));
			System.out.println("Timestamp: "+kv.getTimestamp()+" ");
		}
		table.close();
	}
	
	@SuppressWarnings("deprecation")
	void display() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf,"company");
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		for(Result r:rs) {
			for(KeyValue kv : r.raw()) {
				System.out.println("Name: "+new String(kv.getRow())+" ");
				System.out.print(new String(kv.getFamily())+":");
				System.out.print(new String(kv.getQualifier())+" --> ");
				System.out.println(new String(kv.getValue()));
				System.out.println("Timestamp: "+kv.getTimestamp()+" ");
			}
		}
		table.close();
	}
}
