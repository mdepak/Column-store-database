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
import heap.*;
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
import java.util.Arrays;
import java.util.List;
import java.io.*;
import java.util.Random;

import static tests.ColumnarDriver.runQueryOnColumnar;

//Define the Sample Data schema

class SampleData{

  public String A;
  public String B;
  public int C;
  public int D;

  public SampleData(String _A, String _B, int _C, int _D){
    A = _A;
    B = _B;
    C = _C;
    D = _D;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SampleData that = (SampleData) o;

    if (C != that.C) {
      return false;
    }
    if (D != that.D) {
      return false;
    }

    if(!A.equals(that.A)){
      return false;
    }

    if(!B.equals(that.B)){
      return false;
    }

    return true;
  }

}

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


  public static void runQueryOnColumnar(String columnDBName, String columnFileName,
                                        List<String> columnNames, List<String> valueConstraint, int numBuf, String accessType) {


    try {
        int selectCols[] = new int[columnNames.size()];
        for(int i=0; i<columnNames.size(); i++){
            selectCols[i] = Util.getColumnNumber(columnNames.get(i));
        }


      if(accessType.equals("COLUMNSCAN")) {
        Columnarfile columnarFile = new Columnarfile(columnFileName);
        int numOfColumns = columnarFile.getNumColumns();
        AttrType[] types = columnarFile.getType();

        AttrType[] attrs = new AttrType[1];
        int columnNumber = Util.getColumnNumber(valueConstraint.get(0));
        attrs[0] = types[columnNumber];

        FldSpec[] projlist = new FldSpec[1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);

        String filename = columnFileName + '.' + String.valueOf(columnNumber);

        short[] strSize = new short[numOfColumns];
        int j = 0;
        for(int i=0; i<numOfColumns; i++) {
          if(types[i].attrType == AttrType.attrString) {
            strSize[j] = (short)100;
            j++;
          }
        }
        short[] strSizes = Arrays.copyOfRange(strSize, 0, j);

        CondExpr[] expr = Util.getValueContraint(valueConstraint);
          int selectedCols[] = new int[columnNames.size()];
          for(int i=0; i<columnNames.size(); i++){
              selectedCols[i] = Util.getColumnNumber(columnNames.get(i));
          }
        try {
          ColumnarFileScan columnarFileScan = new ColumnarFileScan(columnFileName, filename, attrs, strSizes, (short) 1, 1, selectedCols,projlist, expr, false);
          Tuple tuple;
          while(true){
            tuple = columnarFileScan.get_next();
            if(tuple == null) break;
            tuple.initHeaders();
            for(int i=0; i<tuple.noOfFlds(); i++){
              if(types[selectCols[i]-1].attrType == AttrType.attrString){
                System.out.println(tuple.getStrFld(i+1));
              }
              if(types[selectCols[i]-1].attrType == AttrType.attrInteger){
                System.out.println(tuple.getIntFld(i+1));
              }
              if(types[selectCols[i]-1].attrType == AttrType.attrReal){
                System.out.println(tuple.getFloFld(i+1));
              }
            }
            System.out.println("");
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      else if(accessType.equals("BTREE")) {
        Columnarfile cf = new Columnarfile(columnFileName);
        int numOfColumns = cf.getNumColumns();
        AttrType[] attrTypes = cf.getType();

        AttrType[] ValueConstraintAttrType = new AttrType[1];
        int columnNumber = Util.getColumnNumber(valueConstraint.get(0)) - 1;
        ValueConstraintAttrType[0] = attrTypes[columnNumber];

        short[] strSize = new short[numOfColumns];
        int j = 0;
        for(int i=0; i<numOfColumns; i++) {
          if(attrTypes[i].attrType == AttrType.attrString) {
            strSize[j] = (short)100;
            j++;
          }
        }
        short[] strSizes = Arrays.copyOfRange(strSize, 0, j);
        /*
        short[] strSizes = new short[2];
        strSizes[0] = 100;
        strSizes[1] = 100;
        */
        ColumnIndexScan colScan;
        CondExpr[] expr = Util.getValueContraint(valueConstraint);
        IndexType indexType = new IndexType(IndexType.B_Index);

        int desiredColumnNumbers[] = new int[columnNames.size()];
        for(int i=0; i<columnNames.size(); i++){
          desiredColumnNumbers[i] = Util.getColumnNumber(columnNames.get(i));
        }

        int indexColumnNumber = Util.getColumnNumber(valueConstraint.get(0));
        String relName = columnFileName + "." + indexColumnNumber;
        String indName = "BTree" + columnFileName + indexColumnNumber;
        boolean indexOnly = desiredColumnNumbers.length == 1 && !valueConstraint.isEmpty() && desiredColumnNumbers[0] == indexColumnNumber;

        try {
          colScan = new ColumnIndexScan(indexType, columnFileName, relName, indName, ValueConstraintAttrType, strSizes, 1, desiredColumnNumbers, expr, indexOnly);
          Columnarfile columnarFile = new Columnarfile(columnFileName);
          AttrType[] types = columnarFile.getType();
          Tuple tuple;
          while(true){
            tuple = colScan.get_next();
            if(tuple == null) break;
            tuple.initHeaders();
            for(int i=0; i<tuple.noOfFlds(); i++){
              if(types[selectCols[i]-1].attrType == AttrType.attrString){
                System.out.println(tuple.getStrFld(i+1));
              }
              if(types[selectCols[i]-1].attrType == AttrType.attrInteger){
                System.out.println(tuple.getIntFld(i+1));
              }
              if(types[selectCols[i]-1].attrType == AttrType.attrReal){
                System.out.println(tuple.getFloFld(i+1));
              }
            }
            System.out.println("");
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }

      else if(accessType.equals("FILESCAN")) {
          int columnNumber = Util.getColumnNumber(valueConstraint.get(0)) - 1;
          String filename = columnFileName + '.' + String.valueOf(columnNumber);
          Columnarfile columnarFile = new Columnarfile(columnFileName);
          int numOfColumns = columnarFile.getNumColumns();
          AttrType[] types = columnarFile.getType();

          AttrType[] attrs = new AttrType[1];
          attrs[0] = types[columnNumber];

          FldSpec[] projlist = new FldSpec[1];
          RelSpec rel = new RelSpec(RelSpec.outer);
          projlist[0] = new FldSpec(rel, 1);

          short[] strSize = new short[numOfColumns];
          int j = 0;
          for(int i=0; i<numOfColumns; i++) {
            if(types[i].attrType == AttrType.attrString) {
              strSize[j] = (short)100;
              j++;
            }
          }
          short[] strSizes = Arrays.copyOfRange(strSize, 0, j);

          CondExpr[] expr = Util.getValueContraint(valueConstraint);

          int selectedCols[] = new int[columnNames.size()];

          for(int i=0; i<columnNames.size(); i++){
              selectedCols[i] = Util.getColumnNumber(columnNames.get(i));
          }

          try {
              ColumnarFileScan columnarFileScan = new ColumnarFileScan(columnFileName, filename, attrs, strSizes, (short) 1, 1, selectedCols, projlist, expr, true);
              Tuple tuple;
              while(true){
                  tuple = columnarFileScan.get_next();
                  if(tuple == null) break;
                  tuple.initHeaders();
                  System.out.println(tuple.getIntFld(1));
              }
          } catch (Exception e) {
              e.printStackTrace();
          }
      }


    } catch (Exception e) {
      System.out.println("Exception in creating index for the columnar database");
      e.printStackTrace();
    }
  }



  private static void createIndexOnColumnarFile(String columnarDatabase, String columnarFile,
      String columnName, String indexType) {

    //TODO: Change to the 1 param constructor
    //Columnarfile file = new Columnarfile(columnarFile);

    try {

      Columnarfile file = new Columnarfile(columnarFile);
      int columnNo = Util.getColumnNumber(columnName);

      switch(indexType)
      {
        case "BTREE":
          file.createBTreeIndex(columnNo);
          break;
        case "BITMAP":
          file.createBitMapIndex(columnNo);
          break;
      }

    } catch (Exception e) {
      System.out.println("Exception in creating index for the columnar database");
      e.printStackTrace();
    }
  }

  @Override
  protected boolean runAllTests() {


    //test1();

    String choice = null;
    String operation;
    String columnDBName ="";
    String columnFileName ="";
    String datafileName ="";
    String columns ="";
    String valConstraint ="";
    int numBuf = 0;
    String accessType ="";
    String indexType ="";
    int noOfCols = 0;
    String[] input = new String[10];
    boolean purge = false;
    do
    {
    System.out.println("-------------------------- MENU ------------------");
    System.out.println("\n\n[0]   Batch Insert (batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS)");
    System.out.println("\n[1]  Index (index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE)");
    System.out.println("\n[2]  Query (query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE)");
    System.out.println("\n[3]  Delete Query (delete COLUMNDBNAME COLUMNARFILENAME VALUECONSTRAINT NUMBUF PURGE)");
    System.out.println("\n[4]  Quit!");
    System.out.println("\nNote: for any value not being specified please mention NA");
    System.out.print("Hi, Please mention the operation in the given format:");
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    List<String> valueConstraint = new ArrayList<String>();
    List<String> columnNames = new ArrayList<String>();
      try {
      choice = in.readLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
    input = choice.split("\\s+");

        if(input[0].contains("quit"))
        {
         continue;
        }
        if(input[0].contains("exit"))
        {
            break;
        }
    operation = input[0];
    if(operation.contains("delete"))
    {
      columnDBName = input[1];
      columnFileName = input[2];
      String colCons = input[3];
      if(colCons != "NA") {
        String opCons = input[4];
        String valCons = input[5];

        valueConstraint.add(colCons);
        valueConstraint.add(opCons);
        valueConstraint.add(valCons);

        //SET THE NUMBUF AND ACCESSTYPE
        numBuf = Integer.parseInt((input[6].contains("NA")) ? "0" : input[6]);
        purge = Boolean.valueOf(input[7]);

      }
      else
      {
        numBuf =Integer.parseInt((input[5].contains("NA")) ? "0": input[5]);;
        purge = Boolean.valueOf(input[6]);
      }

    }
    else if(operation.contains("query"))
    {
      // COLUMN DB NAME
      columnDBName = input[1];
      //COLUMN FILE NAME
      columnFileName = input[2];
      //LIST OF COLUMNS
      columns = input[3];
      columns = columns.replaceAll("\\[", "").replaceAll("\\]","");
      String[] colArray = columns.split(",");
      if(colArray.length > 0 && colArray != null)
      {
        for(String col : colArray)
        {
          columnNames.add(col);
        }
      }
      //VALUECONSTRAINT SPLIT INTO COLUMNAME, OPERATOR AND VALUE AND APPEND IT TO A LIST
      String colCons = input[4];
      //valueConstraint.add(colCons);
        if(colCons.contains("NA"))
        {
            colCons = null;
            valueConstraint.add(colCons);
            numBuf = Integer.parseInt((input[5].contains("NA")) ? "0" : input[5]);;
            accessType = (input[6].contains("NA")) ? null : input[6];
            runQueryOnColumnar(columnDBName, columnFileName, columnNames,  valueConstraint , numBuf, accessType);
        }
        else {
        String opCons = input[5];
        String valCons = input[6];

        valueConstraint.add(colCons);
        valueConstraint.add(opCons);
        valueConstraint.add(valCons);

        //SET THE NUMBUF AND ACCESSTYPE
        numBuf = Integer.parseInt((input[7].contains("NA")) ? "0" : input[7]);
        accessType = (input[8].contains("NA")) ? null : input[8];
        runQueryOnColumnar(columnDBName, columnFileName, columnNames,  valueConstraint , numBuf, accessType);
      }

    }
    else if(operation.contains("batchinsert"))
    {
      datafileName = input[1];
      columnDBName = input[2];
      columnFileName = input[3];
      noOfCols = Integer.parseInt(input[4]);
      try {
        batchInsertQuery(datafileName, columnDBName, columnFileName, noOfCols);
      } catch (Exception e) {
        e.printStackTrace();
      }

    }
    else if(operation.contains("index"))
    {
      columnDBName = input[1];
      columnFileName =input[2];
      String colName = input[3];
      indexType = input[4];
      createIndexOnColumnarFile(columnDBName,columnFileName,colName,indexType);
    }

    }while(!input[0].contains("exit"));

    return true;
  }

  @Override
  protected boolean test1() {
    // Test for the insertion of records in columnar file
    List<SailorDetails> sailors;
    sailors = new ArrayList<>();
    int[] selectedCols = new int[]{0, 0, 0};
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
      //fscan = new ColumnarFileScan("test1.in", attrTypes, strsizes, (short) 1, 1, selectedCols, projlist, expr, false);
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

  protected void batchInsertQuery(String dataFileName, String columnDBName, String columnarFileName, int numOfColumns) throws Exception {
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
          if (Stypes[i-1].attrType == AttrType.attrInteger) {
            t.setIntFld(i, Integer.parseInt(columnValues[i - 1]));
          } else if (Stypes[i-1].attrType == AttrType.attrReal) {
            t.setFloFld(i, Integer.parseInt(columnValues[i - 1]));
          } else if (Stypes[i-1].attrType == AttrType.attrString) {
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
        dbstatus = cd.setupDatabase();

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


