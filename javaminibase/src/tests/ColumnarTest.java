package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.SystemDefs;
import global.TID;
import global.TupleOrder;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import iterator.ColumnarNestedLoopsJoins;
import iterator.ColumnarSort;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

//Define the Sample Data schema

class SampleData {

  public String A;
  public String B;
  public int C;
  public int D;

  public SampleData(String _A, String _B, int _C, int _D) {
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

    if (!A.equals(that.A)) {
      return false;
    }

    if (!B.equals(that.B)) {
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

  public boolean setupDatabase()
      throws
      Exception {

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

    boolean _pass;
    do {

      System.out.print("\n" + "Running columnar tests...." + "\n");
      System.out.println("Setting up the database");
      //Run the tests. Return type different from C++
      _pass = runAllTests();

    } while (_pass != true);

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

// commenting as not required for phase3
//  public static void runDeleteOnColumnar(String columnDBName, String columnFileName,
//      List<String> valueConstraint, int numBuf, boolean purge)
//      throws InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException, SpaceNotAvailableException, InvalidTypeException, FieldNumberOutOfBoundException, HFException, HFDiskMgrException,
//      IndexException,
//      PredEvalException,
//      UnknowAttrType,
//      UnknownIndexTypeException,
//      UnknownKeyTypeException {
//
//    try {
//
//      if (valueConstraint.isEmpty()) {
//        //Delete entire columnar file ??
//        Columnarfile columnarfile = new Columnarfile(columnFileName);
//        columnarfile.deleteColumnarFile();
//        // Display error
//      } else {
//
//        int colnum = Util.getColumnNumber(valueConstraint.get(0));
//        String filename = columnFileName + '.' + String.valueOf(colnum);
//        Columnarfile columnarFile = new Columnarfile(columnFileName);
//
//        CondExpr[] expr = Util.getValueContraint(valueConstraint);
//        int column = tests.Util.getColumnNumber(valueConstraint.get(0));
//
//        List<RID> deleteRID = Util.getRIDListHeapFile(valueConstraint,
//            columnFileName); // get rid list by applying value constraint on designated column
//        List<Integer> positionList = new ArrayList<Integer>();
//
//        for (int i = 0; i < deleteRID.size(); i++) {
//          if (deleteRID.get(i) != null) {
//            int position = columnar.Util
//                .getPositionFromRID(deleteRID.get(i), columnarFile.getColumnFiles()[colnum - 1]);
//            positionList.add(position);
//          }
//        }
//        for (int i = 0; i < positionList.size(); i++) {
//          columnarFile.markTupleDeleted(positionList.get(i));
//        }
//        if (purge) {
//          columnarFile.purgeAllDeletedTuples();
//        }
//      }
//    } catch (Exception e) {
//      System.out.println("Exception in performing delete on columnar database");
//      e.printStackTrace();
//    }
//  }

//TODO update the method to handle multiple conditional expressions
/*
  public static void runQueryOnColumnar(String columnDBName, String columnFileName,
      List<String> columnNames, List<String> valueConstraint, int numBuf, String accessType)
      throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, IndexException, UnknownIndexTypeException, InvalidSlotNumberException, UnknownKeyTypeException {

    try {
      int selectCols[] = new int[columnNames.size()];
      for (int i = 0; i < columnNames.size(); i++) {
        selectCols[i] = Util.getColumnNumber(columnNames.get(i));
      }

      if (accessType.equals("COLUMNSCAN")) {

        int colnum = Util.getColumnNumber(valueConstraint.get(0));
        //String filename = columnFileName + '.' + String.valueOf(colnum);
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
        for (int i = 0; i < numOfColumns; i++) {
          if (types[i].attrType == AttrType.attrString) {
            strSize[j] = (short) 100;
            j++;
          }
        }
        short[] strSizes = Arrays.copyOfRange(strSize, 0, j);

        CondExpr[] expr = Util.getValueContraint(valueConstraint);
        int selectedCols[] = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
          selectedCols[i] = Util.getColumnNumber(columnNames.get(i));
        }
        try {

          ColumnarFileScan columnarFileScan = new ColumnarFileScan(columnFileName, filename, attrs,
              strSizes, (short) 1, 1, selectedCols, projlist, expr, false);
          Tuple tuple;
          while (true) {
            tuple = columnarFileScan.get_next();
            if (tuple == null) {
              break;
            }
            tuple.initHeaders();
            for (int i = 0; i < tuple.noOfFlds(); i++) {
              if (types[selectCols[i] - 1].attrType == AttrType.attrString) {
                System.out.println(tuple.getStrFld(i + 1));
              }
              if (types[selectCols[i] - 1].attrType == AttrType.attrInteger) {
                System.out.println(tuple.getIntFld(i + 1));
              }
              if (types[selectCols[i] - 1].attrType == AttrType.attrReal) {
                System.out.println(tuple.getFloFld(i + 1));
              }
            }
            System.out.println("");
          }
          columnarFileScan.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else if (accessType.equals("BTREE")) {
        Columnarfile cf = new Columnarfile(columnFileName);
        int numOfColumns = cf.getNumColumns();
        AttrType[] attrTypes = cf.getType();

        AttrType[] ValueConstraintAttrType = new AttrType[1];
        int columnNumber = Util.getColumnNumber(valueConstraint.get(0)) - 1;
        ValueConstraintAttrType[0] = attrTypes[columnNumber];

        short[] strSize = new short[numOfColumns];
        int j = 0;
        for (int i = 0; i < numOfColumns; i++) {
          if (attrTypes[i].attrType == AttrType.attrString) {
            strSize[j] = (short) 100;
            j++;
          }
        }
        short[] strSizes = Arrays.copyOfRange(strSize, 0, j);
//        short[] strSizes = new short[2];
//        strSizes[0] = 100;
//        strSizes[1] = 100;
        ColumnIndexScan colScan;
        CondExpr[] expr = Util.getValueContraint(valueConstraint);
        IndexType indexType = new IndexType(IndexType.B_Index);

        int desiredColumnNumbers[] = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
          desiredColumnNumbers[i] = Util.getColumnNumber(columnNames.get(i));
        }

        int indexColumnNumber = Util.getColumnNumber(valueConstraint.get(0));
        String relName = columnFileName + "." + indexColumnNumber;
        String indName = "BTree" + columnFileName + indexColumnNumber;
        boolean indexOnly = desiredColumnNumbers.length == 1 && !valueConstraint.isEmpty()
            && desiredColumnNumbers[0] == indexColumnNumber;

        try {
          colScan = new ColumnIndexScan(indexType, columnFileName, relName, indName,
              ValueConstraintAttrType, strSizes, 1, desiredColumnNumbers, expr, indexOnly);
          Columnarfile columnarFile = new Columnarfile(columnFileName);
          AttrType[] types = columnarFile.getType();
          Tuple tuple;
          while (true) {
            tuple = colScan.get_next();
            if (tuple == null) {
              break;
            }
            tuple.initHeaders();
            for (int i = 0; i < tuple.noOfFlds(); i++) {
              if (types[selectCols[i] - 1].attrType == AttrType.attrString) {
                System.out.println(tuple.getStrFld(i + 1));
              }
              if (types[selectCols[i] - 1].attrType == AttrType.attrInteger) {
                System.out.println(tuple.getIntFld(i + 1));
              }
              if (types[selectCols[i] - 1].attrType == AttrType.attrReal) {
                System.out.println(tuple.getFloFld(i + 1));
              }
            }
            System.out.println("");
          }
          colScan.close();
        } catch (Exception e) {
          e.printStackTrace();
        }

      } else if (accessType.equals("FILESCAN")) {
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
        for (int i = 0; i < numOfColumns; i++) {
          if (types[i].attrType == AttrType.attrString) {
            strSize[j] = (short) 100;
            j++;
          }
        }
        short[] strSizes = Arrays.copyOfRange(strSize, 0, j);

        CondExpr[] expr = Util.getValueContraint(valueConstraint);

        int selectedCols[] = new int[columnNames.size()];

        for (int i = 0; i < columnNames.size(); i++) {
          selectedCols[i] = Util.getColumnNumber(columnNames.get(i));
        }

        try {
          ColumnarFileScan columnarFileScan = new ColumnarFileScan(columnFileName, filename, attrs,
              strSizes, (short) 1, 1, selectedCols, projlist, expr, true);
          Tuple tuple;
          while (true) {
            tuple = columnarFileScan.get_next();
            if (tuple == null) {
              break;
            }
            tuple.initHeaders();
            System.out.println(tuple.getIntFld(1));
          }
          columnarFileScan.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else if (accessType.equals("BITMAP")) {
        Columnarfile file = new Columnarfile(columnFileName);
        int bitMapIndexCol = Util.getColumnNumber(valueConstraint.get(0));

        //TODO: Remove hard coding of the conditional expression and get the parameters from the actual value

        //CondExpr[] condExprs = Util.getValueContraint(valueConstraint);
        CondExpr[] condExprs =  new CondExpr[2];
        condExprs[0] = new CondExpr();
        condExprs[0].type1 = new AttrType(AttrType.attrString);
        Operand operand1 = new Operand();
        operand1.string = "3";
        condExprs[0].operand1 = operand1;

        condExprs[0].type2 = new AttrType(AttrType.attrInteger);
        Operand operand2 = new Operand();
        operand2.integer = 1;
        condExprs[0].operand2 = operand2;

        condExprs[0].op = new AttrOperator(AttrOperator.aopEQ);


        condExprs[1] = new CondExpr();
        condExprs[1].type1 = new AttrType(AttrType.attrString);
        operand1 = new Operand();
        operand1.string = "4";
        condExprs[1].operand1 = operand1;

        condExprs[1].type2 = new AttrType(AttrType.attrInteger);
        operand2 = new Operand();
        operand2.integer = 5;
        condExprs[1].operand2 = operand2;
        
        condExprs[1].op = new AttrOperator(AttrOperator.aopEQ);




        BitmapIterator bitmapIterator = new BitmapIterator(columnFileName,
            selectCols, condExprs);

        //TODO: Fix it after completing and writing proper method
        //Tuple tuple = bitmapIterator.get_next();
        Tuple tuple = bitmapIterator.get_next();

        while (tuple != null) {
          //TODO: Fix it

          System.out.println("Matching position" + tuple.toString());
          tuple = bitmapIterator.get_next();
        }
      }


    } catch (Exception e) {
      System.out.println("Exception in creating index for the columnar database");
      e.printStackTrace();
    }
  }
*/

  private static void createIndexOnColumnarFile(String columnarDatabase, String columnarFile,
      String columnName, String indexType) {

    //TODO: Change to the 1 param constructor
    //Columnarfile file = new Columnarfile(columnarFile);

    try {

      Columnarfile file = new Columnarfile(columnarFile);
      int columnNo = Util.getColumnNumber(columnName);

      switch (indexType) {
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
  protected boolean runAllTests() throws
      Exception {

    //test1();

    String choice = null;
    String operation;
    String columnDBName = "";
    String columnFileName = "";
    String datafileName = "";
    String columns = "";
    String valConstraint = "";
    int numBuf = 0;
    String accessType = "";
    String indexType = "";
    int noOfCols = 0;
    String[] input = new String[10];
    boolean purge = false;

    PCounter.initialize();
    System.out.println("-------------------------- MENU ------------------");
    System.out.println(
        "\n\n[0]   Batch Insert (batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS)");
    System.out.println("\n[1]  Index (index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE)");
    System.out.println(
        "\n[2]  Query (query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE)");
    System.out.println(
        "\n[3]  Delete Query (delete COLUMNDBNAME COLUMNARFILENAME VALUECONSTRAINT NUMBUF PURGE)");
    System.out.println(
        "\n[4]  NestedLoopJoin Query (nlj OUTERTABLENAME INNERTABLENAME LEFTFILTER RIGHTFILTER OUTPUTFILTER INNERACCESSTYPE OUTERACCESSTYPE TARGETFIELDVALUES NUMBUF)");
    System.out.println("\n[4]  Exit!");
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

    operation = input[0];

     /* if (operation.contains("delete")) {

        //delete COLUMNDBNAME COLUMNARFILENAME VALUECONSTRAINT NUMBUF PURGE
        columnDBName = input[1];
        columnFileName = input[2];
        String colCons = input[3];
        if (colCons != "NA") {
          String opCons = input[4];
          String valCons = input[5];

          valueConstraint.add(colCons);
          valueConstraint.add(opCons);
          valueConstraint.add(valCons);

          //SET THE NUMBUF AND ACCESSTYPE
          numBuf = Integer.parseInt((input[6].contains("NA")) ? "0" : input[6]);
          purge = Boolean.valueOf(input[7]);

          // SETUP Database
          Util.createDatabaseIfNotExists(columnDBName, numBuf);

          try {
            runDeleteOnColumnar(columnDBName, columnFileName, valueConstraint, numBuf, purge);
          } catch (Exception e) {

            e.printStackTrace();
          }
        } else {
          colCons = null;
          valueConstraint.add(colCons);
          numBuf = Integer.parseInt((input[5].contains("NA")) ? "0" : input[5]);
          ;
          purge = Boolean.valueOf(input[6]);
          try {
            runDeleteOnColumnar(columnDBName, columnFileName, valueConstraint, numBuf, purge);
          } catch (Exception e) {

            e.printStackTrace();
          }
        }
      } else */
    if (operation.contains("cis")) {
      columnDBName = input[1];
      columnFileName = input[2];
      columns = input[3];
      columns = columns.replaceAll("\\[", "").replaceAll("\\]", "");
      String[] colArray = columns.split(",");
      if (colArray.length > 0 && colArray != null) {
        for (String col : colArray) {
          columnNames.add(col);
        }
        String colCons = input[4];
        if (colCons.contains("NA")) {
          colCons = null;
          valueConstraint.add(colCons);
          numBuf = Integer.parseInt((input[5].contains("NA")) ? "0" : input[5]);
          ;
          accessType = (input[6].contains("NA")) ? null : input[6];

          // SETUP Database
          Util.createDatabaseIfNotExists(columnDBName, numBuf);

          try {

            runColumnarFileScan(columnDBName, columnFileName, columnNames, valueConstraint, numBuf,
                accessType);
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        } else {
          String opCons = input[5];
          String valCons = input[6];

          valueConstraint.add(colCons);
          valueConstraint.add(opCons);
          valueConstraint.add(valCons);

          //SET THE NUMBUF AND ACCESSTYPE
          numBuf = Integer.parseInt((input[7].contains("NA")) ? "100" : input[7]);

          // SETUP Database
          Util.createDatabaseIfNotExists(columnDBName, numBuf);

          accessType = (input[8].contains("NA")) ? null : input[8];
          try {
            runColumnarFileScan(columnDBName, columnFileName, columnNames, valueConstraint, numBuf,
                accessType);
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      }

    } else if (operation.contains("query")) {
      // COLUMN DB NAME
      columnDBName = input[1];
      //COLUMN FILE NAME
      columnFileName = input[2];
      //LIST OF COLUMNS
      columns = input[3];
      columns = columns.replaceAll("\\[", "").replaceAll("\\]", "");
      String[] colArray = columns.split(",");
      if (colArray.length > 0 && colArray != null) {
        for (String col : colArray) {
          columnNames.add(col);
        }
        //VALUECONSTRAINT SPLIT INTO COLUMNAME, OPERATOR AND VALUE AND APPEND IT TO A LIST
        String colCons = input[4];
        //valueConstraint.add(colCons);
        if (colCons.contains("NA")) {
          colCons = null;
          valueConstraint.add(colCons);
          numBuf = Integer.parseInt((input[5].contains("NA")) ? "0" : input[5]);
          ;
          accessType = (input[6].contains("NA")) ? null : input[6];

          // SETUP Database
          Util.createDatabaseIfNotExists(columnDBName, numBuf);

          try {
//TODO uncomment after modifying runQueryOnColumnar
//              runQueryOnColumnar(columnDBName, columnFileName, columnNames, valueConstraint, numBuf,
//                  accessType);
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        } else {
          String opCons = input[5];
          String valCons = input[6];

          valueConstraint.add(colCons);
          valueConstraint.add(opCons);
          valueConstraint.add(valCons);

          //SET THE NUMBUF AND ACCESSTYPE
          numBuf = Integer.parseInt((input[7].contains("NA")) ? "100" : input[7]);

          // SETUP Database
          Util.createDatabaseIfNotExists(columnDBName, numBuf);

          accessType = (input[8].contains("NA")) ? null : input[8];
          try {
//TODO uncomment after modifying runQueryOnColumnar
//              runQueryOnColumnar(columnDBName, columnFileName, columnNames, valueConstraint, numBuf,
//                  accessType);
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      }

    } else if (operation.contains("batchinsert")) {
      datafileName = input[1];
      columnDBName = input[2];
      columnFileName = input[3];
      noOfCols = Integer.parseInt(input[4]);

      // SETUP Database
      Util.createDatabaseIfNotExists(columnDBName, 100);

      try {
        batchInsertQuery(datafileName, columnDBName, columnFileName, noOfCols);
      } catch (Exception e) {
        e.printStackTrace();
      }

    } else if (operation.contains("index")) {
      columnDBName = input[1];
      columnFileName = input[2];
      String colName = input[3];
      indexType = input[4];

      Util.createDatabaseIfNotExists(columnDBName, 100);

      createIndexOnColumnarFile(columnDBName, columnFileName, colName, indexType);
    } else if (operation.contains("nlj")) {
      String outerTableName = input[1];
      String innerTableName = input[2];
      String outerConstraint = input[3];
      CondExpr[] outerConstraintExpr = Util.getCondExprList(outerConstraint);
      String innerConstraint = input[4];
      CondExpr[] innerConstraintExpr = Util.getCondExprList(innerConstraint);
      String joinConstraint = input[5];
      CondExpr[] joinConstraintExpr = Util.getCondExprList(joinConstraint);
      String outerAccessType = input[6];
      String innerAccessType = input[7];
      String targetFieldValues = input[8];
      numBuf = Integer.parseInt(input[9]);

      ColumnarNestedLoopsJoins nljObj = new ColumnarNestedLoopsJoins(outerTableName, innerTableName,
          outerConstraintExpr, innerConstraintExpr, joinConstraintExpr, outerAccessType,
          innerAccessType,
          targetFieldValues, numBuf);
    } else if (operation.contains("sort")) {

      columnDBName = input[1];
      columnFileName = input[2];
      int sortColumn = Util.getColumnNumber(input[3]);

      TupleOrder[] order = new TupleOrder[2];
      order[0] = new TupleOrder(TupleOrder.Ascending);
      order[1] = new TupleOrder(TupleOrder.Descending);

      TupleOrder sortOrder = null;
      if (input[4].equalsIgnoreCase("ASC")) {
        sortOrder = order[0];
      } else {
        sortOrder = order[1];
      }

      int numBuff = Integer.parseInt(input[5]);

      Util.createDatabaseIfNotExists(columnDBName, numBuff);

      performColumnarSort(columnFileName, null, sortColumn, sortOrder, numBuff);


    }

    try {
      SystemDefs.JavabaseBM.flushAllPages();
    } catch (Exception ex) {
      System.out.println("ColumnarTest() flush pages...");
      ex.printStackTrace();
    }

    System.out.println(
        "DiskMgr Read Count = " + PCounter.rcounter + "\t Write Count = " + PCounter.wcounter);

    return input[0].equalsIgnoreCase("exit");
  }


  private void performColumnarSort(
      String columnarFileName,
      int[] selectedCols,
      int sort_fld,
      TupleOrder sort_order,
      int n_pages)
      throws Exception {


    try {

      Columnarfile columnarfile = new Columnarfile(columnarFileName);

      ColumnarSort columnSort = new ColumnarSort(columnarfile, null, sort_fld, sort_order, n_pages);

      AttrType[] types = columnarfile.getType();

      Tuple sortTuple = columnSort.get_next();

      while (sortTuple != null) {

        //Print the tuple
        sortTuple.initHeaders();
        for (int i = 0; i < sortTuple.noOfFlds(); i++) {
          if (types[i].attrType == AttrType.attrString) {
            System.out.println(sortTuple.getStrFld(i + 1));
          }
          if (types[i].attrType == AttrType.attrInteger) {
            System.out.println(sortTuple.getIntFld(i + 1));
          }
          if (types[i].attrType == AttrType.attrReal) {
            System.out.println(sortTuple.getFloFld(i + 1));
          }
        }

        sortTuple = columnSort.get_next();
      }
    }
    catch (Exception ex)
    {
      System.out.println("Exception in the column sort");
      ex.printStackTrace();

      throw ex;
    }
  }

  private void runColumnarFileScan(String columnDBName, String columnFileName,
      List<String> columnNames, List<String> valueConstraint, int numBuf, String accessType) {
    CondExpr[] expr2 = new CondExpr[3];
    expr2[0] = new CondExpr();

    expr2[0].next = null;
    expr2[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr2[0].type1 = new AttrType(AttrType.attrSymbol);

    expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 4);
    expr2[0].type2 = new AttrType(AttrType.attrSymbol);

    expr2[0].type2 = new AttrType(AttrType.attrInteger);
    expr2[0].operand2.integer = 8;

    expr2[1] = new CondExpr();
    expr2[1].op = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].type1 = new AttrType(AttrType.attrSymbol);

    expr2[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
    expr2[1].type2 = new AttrType(AttrType.attrInteger);
    expr2[1].operand2.integer = 8;

    expr2[1].next = new CondExpr();
    expr2[1].next.op = new AttrOperator(AttrOperator.aopEQ);
    expr2[1].next.next = null;
    expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
    expr2[1].next.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
    expr2[1].next.type2 = new AttrType(AttrType.attrString);
    expr2[1].next.operand2.string = "Connecticut";

    expr2[2] = null;

    IndexType[] indexTypes = new IndexType[4];
    indexTypes[0] = (new IndexType(IndexType.B_Index));
    indexTypes[1] = (new IndexType(IndexType.None));
    indexTypes[2] = (new IndexType(IndexType.BIT_MAP));
    indexTypes[3] = (new IndexType(IndexType.B_Index));

    FldSpec[] Sprojection = {
        new FldSpec(new RelSpec(RelSpec.outer), 1),
        new FldSpec(new RelSpec(RelSpec.outer), 2),
        new FldSpec(new RelSpec(RelSpec.outer), 3),
        new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    ColumnarIndexScan columnarIndexScan = null;
    Columnarfile cf = null;
    try {
      cf = new Columnarfile("data");
      columnarIndexScan = new ColumnarIndexScan("data", indexTypes, Sprojection, expr2);
      Tuple tuple;
      AttrType[] types = cf.getType();
      int selectCols[] = new int[columnNames.size()];
      for (int i = 0; i < columnNames.size(); i++) {
        selectCols[i] = Util.getColumnNumber(columnNames.get(i));
      }
      while (true) {
        tuple = columnarIndexScan.get_next();
        if (tuple == null) {
          break;
        }
//        tuple.initHeaders();
//        for (int i = 0; i < tuple.noOfFlds(); i++) {
//          if (types[selectCols[i] - 1].attrType == AttrType.attrString) {
//            System.out.println(tuple.getStrFld(i + 1));
//          }
//          if (types[selectCols[i] - 1].attrType == AttrType.attrInteger) {
//            System.out.println(tuple.getIntFld(i + 1));
//          }
//          if (types[selectCols[i] - 1].attrType == AttrType.attrReal) {
//            System.out.println(tuple.getFloFld(i + 1));
//          }
//        }
        System.out.println("\n");
      }
      columnarIndexScan.close();
    } catch (Exception e) {
      e.printStackTrace();
      columnarIndexScan.close();
    }

  }


  protected void batchInsertQuery(String dataFileName, String columnDBName, String columnarFileName,
      int numOfColumns) throws Exception {
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
    for (int i = 0; i < numOfColumns; i++) {
      String[] columnHeader = headers[i].split(":");
      if (columnHeader[1].contains("int")) {
        Stypes[i] = new AttrType(AttrType.attrInteger);
      } else if (columnHeader[1].contains("float")) {
        Stypes[i] = new AttrType(AttrType.attrReal);
      } else if (columnHeader[1].contains("char")) {
        Stypes[i] = new AttrType(AttrType.attrString);
        int startIndex = columnHeader[1].indexOf("(");
        int endIndex = columnHeader[1].indexOf(")");
        String columnLengthInStr = columnHeader[1].substring(startIndex + 1, endIndex);
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

    //PCounter.initialize();

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
          if (Stypes[i - 1].attrType == AttrType.attrInteger) {
            t.setIntFld(i, Integer.parseInt(columnValues[i - 1]));
          } else if (Stypes[i - 1].attrType == AttrType.attrReal) {
            t.setFloFld(i, Integer.parseInt(columnValues[i - 1]));
          } else if (Stypes[i - 1].attrType == AttrType.attrString) {
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
        throw e;
      }
    }

    //System.out.println("DiskMgr Read Count = " + PCounter.rcounter + "\t Write Count = " + PCounter.wcounter);
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


