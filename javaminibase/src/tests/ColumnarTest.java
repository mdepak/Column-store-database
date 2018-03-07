package tests;

import columnar.Columnarfile;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import global.TID;
import heap.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


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

        t.setFloFld(4, (float) ((sailors.get(i).age)+1));

        f.updateTuple(tid, t);


        Tuple updatedTuple = f.getTuple(tid);

        int sidUp  = updatedTuple.getIntFld(1);
        String snameUp = updatedTuple.getStrFld(2);
        int ratingUp = updatedTuple.getIntFld(3);
        double ageUp = Double.parseDouble(new Float(updatedTuple.getFloFld(4)).toString());

        SailorDetails updatedRecord = new SailorDetails(sidUp, snameUp, ratingUp, ageUp-1);
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
