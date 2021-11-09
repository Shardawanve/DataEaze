
import java.sql.*;

import java.util.Scanner;

public class TableOne
{

	public static void main(String[] args)
	{
		Scanner scan =new Scanner(System.in);
		System.out.println("Enter id");
		int id=scan.nextInt();
		System.out.println("Enter Isconfidential");
		String iso=scan.next();
		System.out.println("Enter city");
		String city=scan.next();
        System.out.println("Enter state");
		int state=scan.nextInt();
		System.out.println("Enter zipcode");
		int zipcode=scan.nextInt();
		System.out.println("Enter country");
		double country=scan.nextDouble();





		String query="insert into Table1 values("+id+",'"+iso+"',"+city+","+state+","+zipcode+","+country+")";

		Connection con=null;
		Statement stmt=null;

		try
		{
			Class.forName("com.mysql.jdbc.Driver");

			con=DriverManager.getConnection("jdbc:mysql://localhost:3307/test", "root", "5035");

			stmt=con.createStatement();
			int count=stmt.executeUpdate(query);
			System.out.println(count+" Records inserted");
		} 
		catch (ClassNotFoundException | SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(con!=null)
			{
				try
				{
					con.close();
				} 
				catch (SQLException e) 
				{
					e.printStackTrace();
				}
			}
			if(stmt!=null)
			{
				try
				{
					stmt.close();
				} 
				catch (SQLException e) 
				{
					e.printStackTrace();
				}
			}
		}



	}
}
