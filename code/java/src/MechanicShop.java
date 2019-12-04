/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Date;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
			
		try{
			System.out.println("Creating entry for new customer");
			Scanner myObj = new Scanner(System.in);

			System.out.print("Please input customer fname: ");
                	String fname = myObj.nextLine();

			System.out.print("Please input customer lname: ");
                	String lname = myObj.nextLine();

			System.out.print("Please input customer phone: ");
                	String phone = myObj.nextLine();

			System.out.print("Please input customer address: ");
                	String address = myObj.nextLine();
			int id = -1;
                        List<List<String>> data = esql.executeQueryAndReturnResult("SELECT MAX (id) FROM Customer;");
			id = Integer.parseInt(data.get(0).get(0)) + 1;
			System.out.println("ID = " + id);
                        String cust = "INSERT INTO Customer VALUES " +
                                      "(" + id + ", '" +
                                      fname + "', '" +
                                      lname + "', '" +
                                      phone + "', '" +
                                      address + "');";
			System.out.println("custinsert = " + cust);
                        esql.executeUpdate(cust);
    
		}catch (Exception e){
                       	System.err.println(e.getMessage());
      	        }

		System.out.println("CUSTOMER INSERTED :D");
	}
	
	public static void AddMechanic(MechanicShop esql){//2
		try{
                        System.out.println("Creating entry for new mechanic");
                        Scanner myObj = new Scanner(System.in);

                        System.out.print("Please input mechanic fname: ");
                        String fname = myObj.nextLine();

                        System.out.print("Please input mechanic lname: ");
                        String lname = myObj.nextLine();

                        System.out.print("Please input mechanic experience: ");
                        String experience = myObj.nextLine();

                        int id = -1;
                        List<List<String>> data = esql.executeQueryAndReturnResult("SELECT MAX (id) FROM Mechanic;");
                        id = Integer.parseInt(data.get(0).get(0)) + 1;
                        System.out.println("ID = " + id);
                        String mech = "INSERT INTO mechanic VALUES " +
                                      "(" + id + ", '" +
                                      fname + "', '" +
                                      lname + "', " +
                                      experience + ");";
                        System.out.println("mechinsert = " + mech);
                        esql.executeUpdate(mech);

                }catch (Exception e){
                        System.err.println(e.getMessage());
                }
		System.out.println("Mechanic inserted");	
	
	}
	
	public static void AddCar(MechanicShop esql){//3
		try{
                        System.out.println("Creating entry for new car");
                        Scanner myObj = new Scanner(System.in);

			System.out.print("Please input Customer ID: ");
                        String cid = myObj.nextLine();

                        System.out.print("Please input car vin: ");
                        String vin = myObj.nextLine();

                        System.out.print("Please input car make: ");
                        String make = myObj.nextLine();

                        System.out.print("Please input car model: ");
                        String model = myObj.nextLine();

                        System.out.print("Please input car year: ");
                        String year = myObj.nextLine();

                        String car = "INSERT INTO Car VALUES " +
                                      "('" + vin + "', '" +
                                      make + "', '" +
                                      model + "', " +
                                      year + ");";
                        System.out.println("carinsert = " + car);
                        esql.executeUpdate(car);

			int id = -1;
			List<List<String>> data = esql.executeQueryAndReturnResult("SELECT MAX (ownership_id) FROM Owns;");
                        id = Integer.parseInt(data.get(0).get(0)) + 1;
                        System.out.println("ownership ID = " + id);

			String owner = "INSERT INTO Owns VALUES " +
                                      "(" + id + ", " +
                                      cid + ", '" +
                                      vin + "');";
                        System.out.println("ownerinsert = " + owner);
                        esql.executeUpdate(owner);
                }catch (Exception e){
                        System.err.println(e.getMessage());
                }
                System.out.println("car inserted");
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		try{
                        System.out.println("Creating entry for new request");
                        Scanner myObj = new Scanner(System.in);

                        System.out.print("Please input customer id: ");
                        String cid = myObj.nextLine();

                        System.out.print("Please input car vin: ");
                        String vin = myObj.nextLine();

                        //System.out.print("Please input date: ");
                        //String date = myObj.nextLine();
			
			Date newdate = new Date();
			String	date = newdate.toString();		

                        System.out.print("Please input car odometer: ");
                        String odom = myObj.nextLine();

			System.out.print("Please input complaint: ");
                        String complain = myObj.nextLine();
                        
			int id = -1;
                        List<List<String>> data = esql.executeQueryAndReturnResult("SELECT MAX (rid) FROM Service_Request;");
                        id = Integer.parseInt(data.get(0).get(0)) + 1;

                        String car = "INSERT INTO Service_Request VALUES " +
                                      "(" + id + ", " +
                                      cid + ", '" +
                                      vin + "', '" +
                                      date + "', " + 
				      odom + ", '" +	
				      complain + "');";
                        System.out.println("carinsert = " + car);
                        esql.executeUpdate(car);
			//int oid = -1;
                        //List<List<String>> odata = esql.executeQueryAndReturnResult("SELECT MAX (ownership_id) FROM Owns;");
                        //oid = Integer.parseInt(odata.get(0).get(0)) + 1;
			//esql.executeUpdate("INSERT INTO Owns VALUES ("+  oid + ", " + cid + ", '" + vin + "') WHERE NOT EXISTS (SELECT o.car_vin FROM Owns o Where o.car_vin = " + vin + ");");   

                }catch (Exception e){
                        System.err.println(e.getMessage());
                }
                System.out.println("SR inserted");	
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		try{
                        System.out.println("Creating entry for closed request");
                        Scanner myObj = new Scanner(System.in);

                        System.out.print("Please input request id: ");
                        String rid = myObj.nextLine();

			List<List<String>> ridcheck = esql.executeQueryAndReturnResult("SELECT COUNT(cr.rid) FROM Closed_Request cr WHERE cr.rid = '" + rid + "';");
                        int counter = 0;
                        counter = Integer.parseInt(ridcheck.get(0).get(0));

                        if(counter > 0){
                                throw new Exception("Already closed...");
                        }


                        System.out.print("Please input mechanic id: ");
                        String mid = myObj.nextLine();

                        //System.out.print("Please input date: ");
                        //String date = myObj.nextLine();

			Date newdate = new Date();
                        String  date = newdate.toString();
                        
			System.out.print("Please input comment: ");
                        String comment = myObj.nextLine();

			System.out.print("Please input bill: ");
                        String bill = myObj.nextLine();

                        int id = -1;
                        List<List<String>> data = esql.executeQueryAndReturnResult("SELECT MAX (wid) FROM Closed_Request;");
                        id = Integer.parseInt(data.get(0).get(0)) + 1;

			//List<List<String>> ridcheck = esql.executeQueryAndReturnResult("SELECT COUNT(cr.rid) FROM Closed_Request cr WHERE cr.rid = '" + rid + "';");  
			//int counter = 0;
			//counter = Integer.parseInt(ridcheck.get(0).get(0));
			
			//if(counter > 0){
			//	throw new Exception("Already closed...");
			//}
			System.out.println("Open, ready to close");
                        String car = "INSERT INTO Closed_Request VALUES " +
                                      "(" + id + ", " +
                                      rid + ", " +
                                      mid + ", '" +
                                      date + "', '" +
                                      comment + "', " +
                                      bill + ");";
                        System.out.println("carinsert = " + car);
                        esql.executeUpdate(car);

                }catch (Exception e){
                        System.err.println(e.getMessage());
                }
                System.out.println("CR inserted");
		
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try {
			esql.executeQueryAndPrintResult("SELECT c.id, c.fname, c.lname, cr.bill FROM Customer c, Service_Request sr, Closed_Request cr WHERE c.id = sr.customer_id AND sr.rid = cr.rid AND cr.bill < 100 ORDER BY cr.bill DESC;");
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try {
                        esql.executeQueryAndPrintResult("SELECT c.id, c.fname, c.lname, count(o.customer_id) FROM Customer c JOIN Owns o ON c.id = o.customer_id GROUP BY c.id HAVING COUNT(o.customer_id) > 20;"); 
                }
                catch (Exception e) {
                        System.err.println(e.getMessage());
                }

	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try {
                        esql.executeQueryAndPrintResult("SELECT c.vin, c.year, c.make, c.model, sr.odometer FROM Car c, Service_Request sr WHERE c.vin = sr.car_vin AND c.year < 1995 AND sr.odometer > 50000 ORDER BY sr.odometer DESC;");
                }
                catch (Exception e) {
                        System.err.println(e.getMessage());
                }
	
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		try {
			Scanner myObj = new Scanner(System.in);
			System.out.print("Please input k: ");
                        String k = myObj.nextLine();
			int lim = Integer.parseInt(k);
                        esql.executeQueryAndPrintResult("SELECT sr.car_vin, c.year, c.make, c.model, COUNT(sr.car_vin)  FROM Service_Request sr, Car c WHERE c.vin = sr.car_vin GROUP BY sr.car_vin, c.year, c.make, c.model ORDER BY COUNT(sr.car_vin) DESC LIMIT " + k + ";");
                }
                catch (Exception e) {
                        System.err.println(e.getMessage());
                }
		
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
		try {
                        esql.executeQueryAndPrintResult("SELECT c.fname, c.lname, SUM(cr.bill) FROM Customer c, Service_Request sr, Closed_Request cr WHERE c.id = sr.customer_id AND sr.rid = cr.rid GROUP BY c.id ORDER BY SUM(cr.bill) DESC;");
                }
                catch (Exception e) {
                        System.err.println(e.getMessage());
                }	
		
	}
	
}
