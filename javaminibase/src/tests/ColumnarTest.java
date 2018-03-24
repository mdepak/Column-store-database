package tests;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import columnar.TupleScan;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.SystemDefs;
import global.TID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import index.ColumnIndexScan;
import iterator.ColumnarFileScan;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.JoinsException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.UnknowAttrType;
import iterator.WrongPermat;
import java.io.IOException;
import java.lang.Exception;
import java.util.ArrayList;
import java.util.List;
import java.io.*;


//Define the SailorDetails schema
class SailorDetails {

  public int sid;
  public String sname;
  public int rating;
  public double age;

  public SailorDetails(int _sid, String _sname, int _rating, double _age) {
    sid = _sid;
    sname = _sname;
    rating = _rating;
    age = _age;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SailorDetails that = (SailorDetails) o;

    if (sid != that.sid) {
      return false;
    }
    if (rating != that.rating) {
      return false;
    }
    if (Double.compare(that.age, age) != 0) {
      return false;
    }
    return sname != null ? sname.equals(that.sname) : that.sname == null;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = sid;
    result = 31 * result + (sname != null ? sname.hashCode() : 0);
    result = 31 * result + rating;
    temp = Double.doubleToLongBits(age);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}

class ColumnarDriver extends TestDriver implements GlobalConst {


  public ColumnarDriver() {
    super("columnartest");
  }

  public boolean setupDatabase() {
    System.out.print("\n" + "Running columnar tests...." + "\n");
    System.out.println("Setting up the database");

    try {
      SystemDefs sysdef = new SystemDefs(dbpath, 8193, 100, "Clock");

    } catch (Exception e) {
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // Kill anything that might be hanging around
    String newdbpath;
    String newlogpath;
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";

    newdbpath = dbpath;
    newlogpath = logpath;

    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;

    // Commands here is very machine dependent.  We assume
    // user are on UNIX system here.  If we need to port this
    // program to other platform, the remove_cmd have to be
    // modified accordingly.
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("" + e);
    }

    remove_logcmd = remove_cmd + newlogpath;
    remove_dbcmd = remove_cmd + newdbpath;

    //This step seems redundant for me.  But it's in the original
    //C++ code.  So I am keeping it as of now, just in case
    //I missed something
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("" + e);
    }

    //Run the tests. Return type different from C++
    boolean _pass = runAllTests();

    //Clean up again
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);

    } catch (IOException e) {
      System.err.println("" + e);
    }

    System.out.print("\n" + "..." + testName() + " tests ");
    System.out.print(_pass == OK ? "completely successfully" : "failed");
    System.out.print(".\n\n");

    return _pass;
  }



  private static void createIndexOnColumnarFile(String columnarDatabase, String columnarFile,
      String columnName, String indexType) {

    //TODO: Change to the 1 param constructor
    //Columnarfile file = new Columnarfile(columnarFile);

    AttrType[] attrTypes = new AttrType[0];
    int columns = 1;
    try {
      Columnarfile file = new Columnarfile(columnarFile, columns, attrTypes);
      int columnNo = Integer.parseInt(columnName);

      switch(indexType)
      {
        case "BTREE":
          file.createBTreeIndex(columnNo);
          break;
        case "BITMAP":
          //file.createBitMapIndex(columnNo);
          break;
      }

    } catch (Exception e) {
      System.out.println("Exception in creating index for the columnar database");
      e.printStackTrace();
    }
  }

  @Override
  protected boolean test1() {
    // Test for the insertion of records in columnar file
    List<SailorDetails> sailors;
    sailors = new ArrayList<>();

    System.out.println("Test 1 - for insertion of records into columnar file");

    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));
    sailors.add(new SailorDetails(53, "Bob Holloway", 9, 53.6));
    sailors.add(new SailorDetails(54, "Susan Horowitz", 1, 34.2));
    sailors.add(new SailorDetails(57, "Yannis Ioannidis", 8, 40.2));
    sailors.add(new SailorDetails(59, "Deborah Joseph", 10, 39.8));
    sailors.add(new SailorDetails(61, "Landwebber", 8, 56.7));
    sailors.add(new SailorDetails(63, "James Larus", 9, 30.3));
    sailors.add(new SailorDetails(64, "Barton Miller", 5, 43.7));
    sailors.add(new SailorDetails(67, "David Parter", 1, 99.9));
    sailors.add(new SailorDetails(69, "Raghu Ramakrishnan", 9, 37.1));
    sailors.add(new SailorDetails(71, "Guri Sohi", 10, 42.1));
    sailors.add(new SailorDetails(73, "Prasoon Tiwari", 8, 39.2));
    sailors.add(new SailorDetails(39, "Anne Condon", 3, 30.3));


    boolean status = true;

    // creating the sailors relation
    AttrType[] Stypes = new AttrType[4];
    Stypes[0] = new AttrType(AttrType.attrInteger);
    Stypes[1] = new AttrType(AttrType.attrString);
    Stypes[2] = new AttrType(AttrType.attrInteger);
    Stypes[3] = new AttrType(AttrType.attrReal);

    //SOS
    short[] Ssizes = new short[1];
    Ssizes[0] = 30; //first elt. is 30

    Tuple t = new Tuple();
    try {
      t.setHdr((short) 4, Stypes, Ssizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    int size = t.size();

    PCounter.initialize();

    // inserting the tuple into file "sailors"
    TID tid;
    Columnarfile f = null;
    try {
      f = new Columnarfile("sailors", 4, Stypes);
    } catch (Exception e) {
      System.err.println("*** error in ColumnarFile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }

    t = new Tuple(size);
    try {
      t.setHdr((short) 4, Stypes, Ssizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }

    for (int i = 0; i < sailors.size(); i++) {
      try {
        t.setIntFld(1, ((SailorDetails) sailors.get(i)).sid);
        t.setStrFld(2, ((SailorDetails) sailors.get(i)).sname);
        t.setIntFld(3, ((SailorDetails) sailors.get(i)).rating);
        t.setFloFld(4, (float) ((SailorDetails) sailors.get(i)).age);
      } catch (Exception e) {
        System.err.println("*** ColumnarFile error in Tuple.setStrFld() ***");
        status = FAIL;
        e.printStackTrace();
      }

      try {
        tid = f.insertTuple(t.returnTupleByteArray());
        Tuple resultantTuple = f.getTuple(tid);

        int sid = resultantTuple.getIntFld(1);
        String sname = resultantTuple.getStrFld(2);
        int rating = resultantTuple.getIntFld(3);
        double age = Double.parseDouble(new Float(resultantTuple.getFloFld(4)).toString());

        SailorDetails retrievedRecord = new SailorDetails(sid, sname, rating, age);
        if (!sailors.get(i).equals(retrievedRecord)) {
          System.err.println(
                  "*** error in ColumnarFile.insertTuple() - retrieved data is not proper based on tuple ID ***");
          status = FAIL;
        }

        t.setFloFld(4, (float) ((sailors.get(i).age) + 1));

        f.updateTuple(tid, t);

        Tuple updatedTuple = f.getTuple(tid);

        int sidUp = updatedTuple.getIntFld(1);
        String snameUp = updatedTuple.getStrFld(2);
        int ratingUp = updatedTuple.getIntFld(3);
        double ageUp = Double.parseDouble(new Float(updatedTuple.getFloFld(4)).toString());

        SailorDetails updatedRecord = new SailorDetails(sidUp, snameUp, ratingUp, ageUp - 1);
        if (!sailors.get(i).equals(updatedRecord)) {
          System.err.println(
                  "*** error in ColumnarFile.insertTuple() - retrieved data is not proper based on tuple ID ***");
          status = FAIL;
        }


      } catch (Exception e) {
        System.err.println("*** error in ColumnarFile.insertRecord() ***");
        status = FAIL;
        e.printStackTrace();
      }
    }

    System.out.println(
        "DiskMgr Read Count = " + PCounter.rcounter + "\t Write Count = " + PCounter.wcounter);

    //TupleScan test

    TupleScan tupleScan = new TupleScan(f);
    TID tid1 = new TID(4);
    Tuple nextTuple = new Tuple();
    while(nextTuple != null){
      nextTuple = tupleScan.getNext(tid1);
    }


    // ColumnarScan

    ColumnarFileScan fscan = null;

    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrInteger);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
    expr[0].operand2.integer = 64;
    expr[0].next = null;
    expr[1] = null;


    FldSpec[] projlist = new FldSpec[1];
    RelSpec rel = new RelSpec(RelSpec.outer);
    projlist[0] = new FldSpec(rel, 1);

    AttrType[] attrTypes = new AttrType[1];
    attrTypes[0] = new AttrType(AttrType.attrInteger);

    short[] strsizes = new short[1];
    strsizes[0] = 100;

    try {
      fscan = new ColumnarFileScan("test1.in", attrTypes, strsizes, (short) 1, 1, projlist, expr);
      Tuple tuple = null;
      while(true){
          tuple = fscan.get_next();
          if(tuple == null) break;
          tuple.initHeaders();
          System.out.println(tuple.getIntFld(1));
      }
    } catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }


    try {
      f.createBTreeIndex(1);
    } catch (Exception ex) {
      status = FAIL;
      ex.printStackTrace();
    }

    try
    {
      f.createBitMapIndex(3);
    }
    catch (Exception ex)
    {
      status = FAIL;
      ex.printStackTrace();
    }

    try {
      // Reopen te file again
      f = new Columnarfile("sailors");
      //Insert data after creating index - to test for index updating
      for (int i = 0; i < sailors.size(); i++) {
        t.setIntFld(1, ((SailorDetails) sailors.get(i)).sid);
        t.setStrFld(2, ((SailorDetails) sailors.get(i)).sname);
        t.setIntFld(3, ((SailorDetails) sailors.get(i)).rating);
        t.setFloFld(4, (float) ((SailorDetails) sailors.get(i)).age);

        tid = f.insertTuple(t.returnTupleByteArray());
      }
    } catch (Exception e) {
      System.err.println("*** ColumnarFile error in Tuple.setStrFld() ***");
      status = FAIL;
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println("*** Error creating relation for sailors");
      Runtime.getRuntime().exit(1);
    }

    if (status) {
      System.out.println("Test 1 successfully completed.");
    } else {
      System.out.println("There is some error in test !!!");
    }
    return status;
  }

  protected void readFileTest(String dataFileName, String columnDBName, String columnarFileName, int numOfColumns) throws Exception {
    File file = new File(dataFileName);
    BufferedReader br = new BufferedReader(new FileReader(file));
    String[] headers;
    String fileLines = br.readLine();
    headers = fileLines.split("\\t");
    if (headers == null) {
      throw new Exception("*** Input file read error! ***");
    }
    int len = headers.length;
    if (len != numOfColumns) {
      throw new Exception("*** Input file, number of columns error! ***");
    }
    AttrType[] Stypes = new AttrType[numOfColumns];
    int numOfStringValues = 0;
    String headerLine = fileLines;
    while (headerLine.contains("char") != false) {
      int index = headerLine.indexOf("char");
      if (index > 0) {
        numOfStringValues++;
        headerLine = headerLine.substring(index + 4);
      }
    }
    short[] strSizes = new short[numOfStringValues];
    int j = 0;
//    List<short> sizes = new ArrayList<short>();
    for(int i=0; i<numOfColumns; i++) {
      String[] columnHeader = headers[i].split(":");
      if(columnHeader[1].contains("int")) {
        Stypes[i] = new AttrType(AttrType.attrInteger);
      } else if(columnHeader[1].contains("float")) {
        Stypes[i] = new AttrType(AttrType.attrReal);
      } else if(columnHeader[1].contains("char")) {
        Stypes[i] = new AttrType(AttrType.attrString);
        int startIndex = columnHeader[1].indexOf("(");
        int endIndex = columnHeader[1].indexOf(")");
        String columnLengthInStr = columnHeader[1].substring(startIndex+1, endIndex);
        strSizes[j] = Short.parseShort(columnLengthInStr);
        j++;
      }
    }
//    short[] strSizes = (short[])sizes.toArray(short[sizes.size()]);

    Tuple t = new Tuple();
    try {
      t.setHdr((short) numOfColumns, Stypes, strSizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      e.printStackTrace();
    }

    int size = t.size();

    PCounter.initialize();

    // inserting the tuple into the Columnar file
    TID tid;
    Columnarfile f = null;
    try {
      f = new Columnarfile(columnarFileName, numOfColumns, Stypes);
    } catch (Exception e) {
      System.err.println("*** error in ColumnarFile constructor ***");
      e.printStackTrace();
    }

    t = new Tuple(size);
    try {
      t.setHdr((short) numOfColumns, Stypes, strSizes);
    } catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      e.printStackTrace();
    }

    while ((fileLines = br.readLine()) != null) {
      try {
        String[] columnValues = fileLines.split("\\t");
        for (int i = 1; i <= numOfColumns; i++) {
          if (Stypes[i].attrType == AttrType.attrInteger) {
            t.setIntFld(i, Integer.parseInt(columnValues[i - 1]));
          } else if (Stypes[i].attrType == AttrType.attrReal) {
            t.setFloFld(i, Integer.parseInt(columnValues[i - 1]));
          } else if (Stypes[i].attrType == AttrType.attrString) {
            t.setStrFld(i, columnValues[i - 1]);
          }
        }
      } catch (Exception e) {
        System.err.println("*** ColumnarFile error in Tuple.setStrFld() ***");
        e.printStackTrace();
      }
      try {
        tid = f.insertTuple(t.returnTupleByteArray());
      } catch (Exception e) {
        System.err.println("*** error in ColumnarFile.insertRecord() ***");
        e.printStackTrace();
      }
    }

    System.out.println(
        "DiskMgr Read Count = " + PCounter.rcounter + "\t Write Count = " + PCounter.wcounter);
  }


  protected String testName() {

    return "Columnar File";
  }
}

public class ColumnarTest {

  public static void main(String[] args) {

    ColumnarDriver cd = new ColumnarDriver();
    boolean dbstatus;
    try {
      cd.setupDatabase();
      dbstatus = cd.runTests();

      if (dbstatus != true) {
        System.err.println("Error encountered during columnar file tests:\n");
        Runtime.getRuntime().exit(1);
      }

      Runtime.getRuntime().exit(0);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}

class ColumnIndexScanTest {

  private static short REC_LEN1 = 32;
  private static short REC_LEN2 = 160;

  public void get_next() throws Exception {
    AttrType[] attrType = new AttrType[2];
    attrType[0] = new AttrType(AttrType.attrString);
    attrType[1] = new AttrType(AttrType.attrString);
    short[] attrSize = new short[2];
    attrSize[0] = REC_LEN2;
    attrSize[1] = REC_LEN1;
    ColumnIndexScan colScan = null;
    // set up an identity selection
    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrString);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    expr[0].operand2.string = "dsilva";
    expr[0].next = null;
    expr[1] = null;
    colScan = new ColumnIndexScan(new IndexType(IndexType.B_Index), "test1.in", "BTreeIndex",
        attrType, attrSize, expr, true);
  }

  public void close() throws Exception {
  }
}