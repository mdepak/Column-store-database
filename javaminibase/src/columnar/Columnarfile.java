package columnar;

import btree.BTreeFile;
import btree.KeyClass;
import global.AttrType;
import global.RID;
import global.TID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Columnarfile {

  //Field Summary
  static int numColumns;
  AttrType[] type;
  private short[] strSizes;

  private String fileName;
  private Heapfile[] columnFiles;
  private String[] heapFileNames;
  private Heapfile headerFile;


  private boolean[] hasBTreeIndex;
  private List<ColumnarHeaderRecord> columnarHeaderRecords;


  /**
   * Initialize: if columnar file does not exits, create one heapfile (‘‘name.columnid’’) per
   * column; also create a ‘‘name.hdr’’ file that contains relevant metadata.
   */
  public Columnarfile(java.lang.String name, int numColumns, AttrType[] type)
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTypeException, SpaceNotAvailableException, FieldNumberOutOfBoundException, InvalidSlotNumberException, InvalidTupleSizeException {

    //TODO: Create heap files according - implement it

    if (name != null) {
      fileName = name;
      this.numColumns = numColumns;
      this.type = type;
    }

    heapFileNames = new String[numColumns];
    columnFiles = new Heapfile[numColumns];
    hasBTreeIndex = new boolean[numColumns];
    strSizes = new short[1];
    columnarHeaderRecords = new ArrayList<>();

    initHeaderFile();

    for (int i = 0; i < numColumns; i++) {
      heapFileNames[i] = fileName + "." + (i + 1);
      columnFiles[i] = new Heapfile(heapFileNames[i]);
    }
  }

  private void initHeaderFile()
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException, SpaceNotAvailableException, InvalidSlotNumberException {
      headerFile = new Heapfile(fileName+".hdr");

      for(int colNo = 1 ; colNo <= numColumns; colNo++)
      {
        Tuple dfileTuple = new ColumnarHeaderRecord(FileType.DATA_FILE, colNo, type[colNo-1],fileName + "." + colNo, null, 0).getTuple();
        headerFile.insertRecord(dfileTuple.getTupleByteArray());
      }
  }

  /**
   * Delete all relevant files from the database.
   */
  void deleteColumnarFile() {

  }

  /**
   * Insert tuple into file, return its tid
   */
  public TID insertTuple(byte[] tuplePtr)
      throws IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {

    // Input byte array has all the column values for a record.
    // Now the column values have to be separated and corresponding tuples should be created for insertion
    // into the column based heap files

    Tuple rowTuple = new Tuple(tuplePtr);

    int position = 0; // Position need to maintained somehow in the meta data so that it can be updated.
    RID[] resultRIDs = new RID[numColumns];

    for (int i = 0; i < numColumns; i++) {
      Tuple columnTuple = Util.createColumnarTuple(rowTuple, i + 1, type[i]);
      resultRIDs[i] = columnFiles[i].insertRecord(columnTuple.getTupleByteArray());
    }

    return new TID(numColumns, position, resultRIDs);
  }


  /**
   * Read the tuple with the given tid from the columnar file
   */
  public Tuple getTuple(TID tid) throws Exception {

    Tuple[] columnarTuples = new Tuple[numColumns];

    //Get the tuples from each columnar heap file
    List<Integer> stringSizes = new ArrayList<>();

    ValueClass[] values = new ValueClass[numColumns];
    for (int field = 1; field <= numColumns; field++) {
      ValueClass val = getValue(tid, field);

      values[field - 1] = val;
      if (val instanceof StringValue) {
        stringSizes.add(((String) val.getValue()).length());
      }
    }

    // Merge all the columns values and form the row tuple
    //TODO: Init tuple with specific size rather than default 1024
    Tuple rowTuple = new Tuple();
    short[] strSizes = new short[stringSizes.size()];
    for (int idx = 0; idx < stringSizes.size(); idx++) {
      strSizes[idx] = (short) stringSizes.get(idx).intValue();
    }

    // Set the header of the tuple
    rowTuple.setHdr((short) numColumns, type, strSizes);

    int fieldNo = 1;
    for (ValueClass value : values) {
      value.setValueinRowTuple(rowTuple, fieldNo++);
    }

    return rowTuple;
  }


  /**
   * Read the value with the given column and tid from the columnar file
   */
  public ValueClass getValue(TID tid, int column) throws Exception {
    Tuple tuple = columnFiles[column - 1].getRecord(tid.getRID(column - 1));

    //Init header attributes in the tuple as the result of heap file getRecord does not set those attributes
    tuple.initHeaders();

    ValueClass value = Util.valueClassFactory(type[column - 1]);
    value.setValueFromColumnTuple(tuple, 1);
    return value;
  }

  /**
   * Return the number of tuples in the columnar file.
   */
  int getTupleCnt() {

    return 0;
  }

  /**
   * Initiate a sequential scan of tuples.
   */
  TupleScan openTupleScan() {
    return null;
  }

  /**
   * Initiate a sequential scan along a given column.
   */
  Scan openColumnScan(int columnNo) {
    return null;
  }

  /**
   * Updates the specified record in the columnar file.
   */
  public boolean updateTuple(TID tid, Tuple newtuple) throws Exception {

    for(int field =1; field<=numColumns; field++) {
      boolean status = updateColumnofTuple(tid, newtuple, field);
      if(!status) {
        return status;
      }
    }
    return true;
  }

  /**
   * Updates the specified column of the specified record in the columnar file
   */
  public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
      throws Exception {
    Tuple columnTuple = Util.createColumnarTuple(newtuple, column, type[column-1]);
    return columnFiles[column-1].updateRecord(tid.getRID(column-1), columnTuple);
  }

  /**
   * if it doesn’t exist, create a BTree index for the given column
   */
  public boolean createBTreeIndex(int column)
      throws Exception {

    // Check all the files it contains

    //TODO: Modify delete fashion if necessary
    int keySize = getKeySize(column);
      BTreeFile btf = new BTreeFile("BTree" + fileName + column, type[column - 1].attrType,
          keySize, 1);//full delete

    Scan scan = new Scan(columnFiles[column-1]);
    RID rid = new RID();

    Tuple temp = null;

    ValueClass valueClass = Util.valueClassFactory(type[column - 1]);
    try {
      temp = scan.getNext(rid);
    } catch (Exception e) {
      e.printStackTrace();
    }

    while (temp != null) {
      // Copy to another variable so that the fields of the tuple are initialized.

      Tuple t = new Tuple(temp.getTupleByteArray());
      t.tupleCopy(temp);
      KeyClass key = valueClass.getKeyClassFromColumnTuple(t, 1);
      btf.insert(key, rid);

      temp = scan.getNext(rid);
    }
    return true;
  }

  int getKeySize(int column)
  {
    int strPtr = 0;
    for(int i = 0; i< column-1; i++)
    {
      if(type[i].attrType == AttrType.attrString)
        strPtr++;
    }

    AttrType attrType = type[column-1];
    int keySize = 0;

    switch (attrType.attrType)
    {
      case AttrType.attrInteger:
        keySize = 4;
        break;
      case AttrType.attrReal:
        keySize = 4;
        break;
      case AttrType.attrString:
        keySize = strSizes[strPtr];
    }

    return keySize;
  }

  /**
   * if it doesn’t exist, create a bitmap index for the given column and value
   */
  boolean createBitMapIndex(int columnNo, ValueClass value) {
   // BitMapFile file = new BitMapFile("", this,columnNo,value);
    return false;
  }

  /**
   * add the tuple to a heapfile tracking the deleted tuples from the columnar file
   */
  boolean markTupleDeleted(TID tid) {
    return false;
  }

  /**
   * merge all deleted tuples from the file as well as all from all index files.
   */
  boolean purgeAllDeletedTuples() {
    return false;
  }
}
