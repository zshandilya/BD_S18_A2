package dml;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDmlDemo {

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		Scanner s = new Scanner(System.in);
		int op;
		String name,age,dept;
		HbaseDml z = new HbaseDml();
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		@SuppressWarnings("deprecation")
		HTableDescriptor des = new HTableDescriptor(Bytes.toBytes("company"));
		des.addFamily(new HColumnDescriptor("details"));
		
		//Check if table already exists
		if(admin.tableExists("company")) {
			System.out.println("Table already exists. Disabling it ...");
			admin.disableTable("company");
			admin.deleteTable("company");
			System.out.println("Existing table has been deleted.");
		}
		admin.createTable(des); //create table
		System.out.println("CREATED TABLE COMPANY WITH COLUMN FAMILY: details");
		
		do {
		
			System.out.println("****** HBASE OPERATIONS ******");
			System.out.println("1. Enter a record");
			System.out.println("2. View a record");
			System.out.println("3. View table");
			System.out.println("4. Exit");
			System.out.print("Enter Your Option(1-4): ");
			op=s.nextInt();
			
			switch(op) {
			
			case 1 :
				System.out.print("Enter name of employee: ");
				name=s.next();
				System.out.print("Enter age of employee: ");
				age=s.next();
				System.out.print("Enter department of employee: ");
				dept=s.next();
				z.insert(name,age,dept); // Insert new record
				break;
					
			case 2 :
				System.out.println("Enter the name of employee: ");
				name=s.next();
				z.getRecord(name); // display record
				break;
			
			case 3 :
				z.display(); // display hbase table
				break;
					
			case 4 :
				System.out.println("Thank you for using the Application. Good Bye ...");
				break;
				
			default:
				System.out.println("Invalid Option. Please Try Again ...");
				break;
				
			};
		} while (op!=4);
		s.close(); // closing scanner object
		admin.close(); // closing HBaseAdmin object
	}

}
