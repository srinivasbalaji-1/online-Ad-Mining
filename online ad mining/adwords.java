import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import oracle.jdbc.OracleTypes;


public class adwords {
	public static void main(String args[])
	{
		char[] outFileMapper = {' ','1','3','5','2','4','6'};
		int[] task = new int[7];
		try {
			File f1 = new File("system.in");
			BufferedReader r= new BufferedReader(new FileReader(f1));
			String username = r.readLine().split(" = ")[1];
			String password = r.readLine().split(" = ")[1];
			task[1] = Integer.parseInt(r.readLine().split(" = ")[1]);
			task[3] = Integer.parseInt(r.readLine().split(" = ")[1]);
			task[5] = Integer.parseInt(r.readLine().split(" = ")[1]);
			task[2] = Integer.parseInt(r.readLine().split(" = ")[1]);
			task[4] = Integer.parseInt(r.readLine().split(" = ")[1]);
			task[6] = Integer.parseInt(r.readLine().split(" = ")[1]);
			r.close();
			
			for(int i=1;i<=6;i++)
			{       
				System.out.println("sqlplus "+username+ "@orcl/"+password+" @adwords.sql");
				
				if(i==1)
				{
					Process p;// = Runtime.getRuntime().exec("source /usr/local/etc/ora11.csh");
					//p.waitFor();
					ProcessBuilder pb = new ProcessBuilder("sqlplus",username+"@orcl/"+password,"@./adwords.sql");
					//Process p = Runtime.getRuntime().exec("sqlplus "+username+"/"+password+"@orcl "+" @adwords.sql");
					//p.waitFor();
					p = pb.start();
					p.waitFor();
					pb = new ProcessBuilder("sqlldr",username+"@orcl/"+password,"control=ctl.txt");
					pb.start().waitFor();
					pb = new ProcessBuilder("sqlldr",username+"@orcl/"+password,"control=ad.txt");
					pb.start().waitFor();
					pb = new ProcessBuilder("sqlldr",username+"@orcl/"+password,"control=key.txt");
					pb.start().waitFor();
					System.out.println("Done loading procedure and data");
					DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
				}	
				File f = new File("system.out."+outFileMapper[i]);
				BufferedWriter w = new BufferedWriter(new FileWriter(f));
				Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@oracle1.cise.ufl.edu:1521:orcl",username,password);
				CallableStatement cs = conn.prepareCall("{call getads(?,?,?)}");
				cs.registerOutParameter(1, OracleTypes.CURSOR);
				cs.setInt(2, i);
				cs.setInt(3,task[i]);
				cs.executeUpdate();
				ResultSet rs = (ResultSet)cs.getObject(1);
				System.out.println("Done task"+outFileMapper[i]);
				while(rs.next())
				{
					w.write(rs.getInt(1)+", "+(int)rs.getFloat(2)+", "+rs.getInt(3)+", "+rs.getFloat(4)+", "+rs.getFloat(5)+"\n");
				}
				rs.close();
				conn.close();
				w.close();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
