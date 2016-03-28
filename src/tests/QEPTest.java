package tests;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;
import java.util.LinkedList;
import java.util.Scanner;
import java.io.FileReader;

// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {	
	/** The display name of the test suite. */
	private static final String TEST_NAME = "Query Evaulation Pipeline Tests";

	/** Size of tables in test3. */
	private static final int SUPER_SIZE = 2000;
	/* Schema for employee file */
	private static Schema s_employee;
	/*Schema for department file */
	private static Schema s_department;

	private static HeapFile empHf;
	private static HeapFile deptHf;

	public static void main(String argv[]) {
		// create a clean Minibase instance
		QEPTest qep = new QEPTest();
		qep.create_minibase();

		// initialize schema for the "employees" table
		s_employee = new Schema(5);
		s_employee.initField(0, AttrType.INTEGER, 4, "EmpId");
		s_employee.initField(1, AttrType.STRING, 40, "Name");
		s_employee.initField(2, AttrType.FLOAT, 4, "Age");
		s_employee.initField(3, AttrType.FLOAT, 4, "Salary");
		s_employee.initField(4, AttrType.INTEGER, 4, "DeptID");

		// initialize schema for the "Department" table
		s_department = new Schema(4);
		s_department.initField(0, AttrType.INTEGER, 4, "DeptId");
		s_department.initField(1, AttrType.STRING, 40, "Name");
		s_department.initField(2, AttrType.FLOAT, 10, "MinSalary");
		s_department.initField(3, AttrType.FLOAT, 10, "MaxSalary");

		//read data from file using argv[0] as a path
		Scanner empSc = null;
		Scanner deptSc = null;

		try {
			empSc = new Scanner(new FileReader(argv[0] + "/Employee.txt"));
			deptSc = new Scanner(new FileReader(argv[0] + "/Department.txt"));
		} catch(Exception e) {
			System.out.println("File does not exist!");
		}
		
		if(empSc.hasNextLine())
			empSc.nextLine(); //reads first line of Employee text

		if(deptSc.hasNextLine())
			deptSc.nextLine(); //reads first line of Department text
			
		//Placeholders for getting data from employee table
		String[] empArr = null;
	 	Tuple empTuple = new Tuple(s_employee);
		empHf = new HeapFile("Employees.txt");

		while(empSc.hasNextLine()) {
			empArr = empSc.nextLine().split(",");
			empTuple.setAllFields(Integer.parseInt(empArr[0].trim()), empArr[1].trim(), Float.parseFloat(empArr[2].trim()), Float.parseFloat(empArr[3].trim()), Integer.parseInt(empArr[4].trim()));
			empTuple.insertIntoFile(empHf);
		}

		//Placeholders for getting data from department table
		String[] deptArr = null;
	 	Tuple deptTuple = new Tuple(s_department);
		deptHf = new HeapFile("Department.txt");

		while(deptSc.hasNextLine()) {
			deptArr = deptSc.nextLine().split(",");
			deptTuple.setAllFields(Integer.parseInt(deptArr[0].trim()), deptArr[1].trim(), Float.parseFloat(deptArr[2].trim()), Float.parseFloat(deptArr[3].trim()));
			deptTuple.insertIntoFile(deptHf);
		}

		// run all the test cases
		System.out.println("\n" + "Running " + TEST_NAME + "...");
		boolean status = PASS;
		//status &= qep.test1();
		//status &= qep.test2();
		//status &= qep.test3();
		status &= qep.test4();
		status &= qep.test5();
		// display the final results
		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		} else {
			System.out.println("All " + TEST_NAME
					+ " completed; verify output for correctness.");
		}

	} // public static void main (String argv[])

	protected boolean test1() {
		try {

			System.out.println("\nTest 1:  Projection operator, DISPLAY FOR EACH EMPLOYEE HIS ID, NAME, AGE\n");
			initCounts();
			
			// test projection operator
			saveCounts(null);
			FileScan scan = new FileScan(s_employee, empHf);
			Projection pro = new Projection(scan, 0, 1, 2);
			pro.execute();

			// destroy temp files before doing final counts
			pro = null;
			scan = null;
			System.gc();
			saveCounts("projemp");

			// that's all folks!
			System.out.print("\n\nTest 1 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 1 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(2);
			System.out.println();
		}

	}

	protected boolean test2() {
		try {

			System.out.println("\nTest 2:  DISPLAY NAME FOR DEPT WHERE MINSALARY = MAXSALARY\n");
			initCounts();
		
			// test selection onto projection
			saveCounts(null);
			FileScan scan = new FileScan(s_department, deptHf);
			Selection sel = new Selection(scan, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 3));
			Projection pro = new Projection(sel, 1);
			pro.execute();

			// destroy temp files before doing final counts
			pro = null;
			sel = null;
			scan = null;
			System.gc();
			saveCounts("both");

			// that's all folks!
			System.out.print("\n\nTest 2 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 2 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(2);
			System.out.println();
		}

	}

	protected boolean test3() {
		try {

			System.out.println("\nTest 3:  FOR EACH EMPLOYEE, DISPLAY HIS NAME, NAME OF DEPT, MAX SALARY OF DEPT");
			initCounts();
		
			// test selection onto projection
			saveCounts(null);

			SimpleJoin join = new SimpleJoin(new FileScan(s_employee, empHf),
					new FileScan(s_department, deptHf), new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 5));
			Projection pro = new Projection(join,1,6,8);
			pro.execute();

			// destroy temp files before doing final counts
			pro = null;
			join = null;
			System.gc();
			saveCounts("join");

			// that's all folks!
			System.out.print("\n\nTest 3 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 3 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(2);
			System.out.println();
		}

	}

	protected boolean test4() {
		try {

			System.out.println("\nTest 4:  FOR EACH EMPLOYEE, DISPLAY THIER NAME FOR WHOMEVER' SALARY IS GRETER THAN THEIR DEPT'S MAX SALR");
			initCounts();
		
			// test selection onto projection
			saveCounts(null);
			SimpleJoin join = new SimpleJoin(new FileScan(s_employee, empHf),
					new FileScan(s_department, deptHf), new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 5));

			Selection sel = new Selection(join, new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FIELDNO, 8));
			Projection pro = new Projection(sel, 1);
			pro.execute();

			// destroy temp files before doing final counts
			pro = null;
			sel = null;
			join = null;
			System.gc();
			saveCounts("test4");

			// that's all folks!
			System.out.print("\n\nTest 4 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 4 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(2);
			System.out.println();
		}

	}

protected boolean test5() {
		try {

			System.out.println("\nTest 5:  Index scan test");
			initCounts();
		
			// test selection onto projection
			saveCounts(null);
			System.out.println("FILE NAME!!!!!!!!!!!!!!!!: : " +  empHf.toString());
			HashJoin join = new HashJoin(new IndexScan(s_employee, new HashIndex(empHf.toString()), empHf),
					new IndexScan(s_department, new HashIndex(deptHf.toString()),deptHf), 4,5);

			Selection sel = new Selection(join, new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FIELDNO, 8));
			Projection pro = new Projection(sel, 1);
			pro.execute();

			// destroy temp files before doing final counts
			pro = null;
			sel = null;
			join = null;
			System.gc();
			saveCounts("test4");

			// that's all folks!
			System.out.print("\n\nTest 4 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 4 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(2);
			System.out.println();
		}

	}
}
