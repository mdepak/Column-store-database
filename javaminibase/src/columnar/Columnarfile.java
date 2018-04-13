package columnar;

import bitmap.BMBufMgrException;
import bitmap.BMException;
import bitmap.BitMapFile;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrType;
import global.PageId;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Columnarfile {

  //Field Summary
  static int numColumns;
  AttrType[] type;
  private short[] strSizes;

  private String fileName;
  private Heapfile[] columnFiles;
  private String[] heapFileNames;
  private Heapfile headerFile;
  String infoHeaderFileName;


  public short[] getStrSizes()
  {
    return strSizes;
  }

  private List<ColumnarHeaderRecord> columnarHeaderRecords;

  private Map<Integer, String> bTreeIndexes = new HashMap<>();
  private Map<Integer, List<ColumnarHeaderRecord>> bitmapIndexes = new HashMap<>();
  private Map<String, Integer> attrNameColNoMapping = new HashMap();

  public int getColumnNumber(String key) {
    return ((int)attrNameColNoMapping.get(key));
  }


  public List<ColumnarHeaderRecord> getBitMapIndicesInfo(int colNo)
  {
    return bitmapIndexes.get(colNo);
  }

  /**
   * This constructor is called when the columnar file is already created.
   */
  public Columnarfile(String fileName)
      throws HFDiskMgrException, InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException {
    //TODO: Columnar file - if the file already exists then then get the information from the header file and then do the work
    this.fileName = fileName;
    loadInfoHeaderFileData();

    heapFileNames = new String[numColumns];
    columnFiles = new Heapfile[numColumns];


    for (int i = 0; i < numColumns; i++) {
      heapFileNames[i] = fileName + "." + (i + 1);
      columnFiles[i] = new Heapfile(heapFileNames[i]);
    }

  }

  private String getDeleteFileName() {
    return fileName + ".del";
  }

  public AttrType[] getType(){
    return type;
  }

  public int getNumColumns() { return numColumns;}

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

    strSizes = new short[2];
    strSizes[0] = 50;
    strSizes[1] = 50;
    columnarHeaderRecords = new ArrayList<>();

    //TODO: Init headers on demand basis - no need to load the data every time - in case of insert

    PageId firstDirPage = Heapfile.get_file_entry(getInfoHeaderFileName());
    // Init the column related details to the file only once.
    if(firstDirPage == null) {
      initHeaderFile();
    }

    for (int i = 0; i < numColumns; i++) {
      heapFileNames[i] = fileName + "." + (i + 1);
      columnFiles[i] = new Heapfile(heapFileNames[i]);
    }
  }



  private String getInfoHeaderFileName()
  {
    return fileName + ".hdr";
  }

  public Heapfile[] getColumnFiles() {
    return columnFiles;
  }

  private void initHeaderFile()
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException, SpaceNotAvailableException, InvalidSlotNumberException {
    infoHeaderFileName = getInfoHeaderFileName();
    headerFile = new Heapfile(infoHeaderFileName);

    //TODO: Insert the headers info only if the file already does not exist - do it only once.
    int strPtr = 0;
    for (int colNo = 1; colNo <= numColumns; colNo++) {

      int maxValSize = 0;
      if (type[colNo - 1].attrType == AttrType.attrString) {
        maxValSize = strSizes[strPtr++];
      }

      Tuple dfileTuple = new ColumnarHeaderRecord(FileType.DATA_FILE, colNo, type[colNo - 1],
          fileName + "." + colNo, getDummyValue(type[colNo - 1]), maxValSize).getTuple();

      headerFile.insertRecord(dfileTuple.getTupleByteArray());
    }

    bitmapIndexes = new HashMap<>();
    bTreeIndexes = new HashMap<>();

    //loadInfoHeaderFileData();
  }

  private void loadInfoHeaderFileData()
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, FieldNumberOutOfBoundException {

    infoHeaderFileName = fileName + ".hdr";
    headerFile = new Heapfile(infoHeaderFileName);
    bitmapIndexes = new HashMap<>();
    bTreeIndexes = new HashMap<>();


    Scan scan = new Scan(headerFile);
    columnarHeaderRecords = new ArrayList<>();

    RID rid = new RID();
    Tuple temp = null;

    List<ColumnarHeaderRecord> columnarDataFiles = new ArrayList();

    try {
      temp = scan.getNext(rid);
    } catch (Exception e) {
      e.printStackTrace();
    }

    while (temp != null) {
      // Copy to another variable so that the fields of the tuple are initialized.

      Tuple tuple = new Tuple(temp.getTupleByteArray());
      tuple.tupleCopy(temp);


      ColumnarHeaderRecord record = ColumnarHeaderRecord.getInstanceFromInfoTuple(tuple);
      columnarHeaderRecords.add(record);

      if (record.getFileType() == FileType.BTREE_FILE) {
        bTreeIndexes.put(record.getColumnNo(), record.getFileName());
      } else if (record.getFileType() == FileType.BITMAP_FILE) {
        if (bitmapIndexes.get(record.getColumnNo()) == null) {
          bitmapIndexes.put(record.getColumnNo(), new ArrayList<>());
        }

        bitmapIndexes.get(record.getColumnNo()).add(record);
      } else if (record.getFileType() == FileType.DATA_FILE) {
        columnarDataFiles.add(record);
      }

      temp = scan.getNext(rid);
    }

    numColumns = columnarDataFiles.size();
    //numColumns = 4;

    type = new AttrType[numColumns];

    List<Integer> strAttrSizes = new ArrayList();

    for (int idx = 0; idx < numColumns; idx++) {
      type[idx] = columnarDataFiles.get(idx).getAttrType();
      if (type[idx].attrType == AttrType.attrString) {
        strAttrSizes.add(columnarDataFiles.get(idx).getMaxValSize());
      }
    }

    strSizes = new short[strAttrSizes.size()];
    for (int idx = 0; idx < strAttrSizes.size(); idx++) {
      strSizes[idx] = (short) (int) strAttrSizes.get(idx);
    }

    /*type = new AttrType[4];
    type[0] = new AttrType(AttrType.attrString);
    type[1] = new AttrType(AttrType.attrString);
    type[2] = new AttrType(AttrType.attrInteger);
    type[3] = new AttrType(AttrType.attrInteger);


    strSizes = new short[2];

    strSizes[0] = 25;
    strSizes[1] = 25;
  */

    scan.closescan();
  }

  /**
   * Delete all relevant files from the database.
   */
  public void deleteColumnarFile() {

  }

  /**
   * Insert tuple into file, return its tid
   */
  public TID insertTuple(byte[] tuplePtr)
      throws IOException, InvalidTypeException, FieldNumberOutOfBoundException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException, UnpinPageException, LeafDeleteException, LeafInsertRecException, KeyTooLongException, ConvertException, PinPageException, DeleteRecException, IndexSearchException, GetFileEntryException, IndexInsertRecException, NodeNotMatchException, KeyNotMatchException, ConstructPageException, IteratorException, InsertException, BMBufMgrException, BMException, AddFileEntryException {

    // Input byte array has all the column values for a record.
    // Now the column values have to be separated and corresponding tuples should be created for insertion
    // into the column based heap files

    Tuple rowTuple = new Tuple(tuplePtr);

    int position = 0; // Position need to maintained somehow in the meta data so that it can be updated.
    RID[] resultRIDs = new RID[numColumns];

    for (int i = 0; i < numColumns; i++) {
      Tuple columnTuple = Util.createColumnarTuple(rowTuple, i + 1, type[i]);
      resultRIDs[i] = columnFiles[i].insertRecord(columnTuple.getTupleByteArray());

      position = columnFiles[i].getLastInsertedPosition();

      //Update Btree Index file if exists
      updateBtreeIndexIfExists(i + 1, columnTuple, resultRIDs[i]);

      //Update BitMap index file if exits
      updateBitMapIndexIfExists(i + 1, columnTuple, position);
    }

    return new TID(numColumns, position, resultRIDs);
  }

  private void updateBtreeIndexIfExists(int column, Tuple columnarTuple, RID rid)
      throws ConstructPageException, GetFileEntryException, PinPageException, IOException, FieldNumberOutOfBoundException, IteratorException, NodeNotMatchException, UnpinPageException, LeafInsertRecException, IndexSearchException, InsertException, ConvertException, DeleteRecException, KeyNotMatchException, LeafDeleteException, KeyTooLongException, IndexInsertRecException {
    String treeFileName = bTreeIndexes.get(column);
    if (treeFileName != null) {
      BTreeFile bTreeFile = new BTreeFile(treeFileName);
      ValueClass valueClass = Util.valueClassFactory(type[column - 1]);
      KeyClass key = valueClass.getKeyClassFromColumnTuple(columnarTuple, 1);
      bTreeFile.insert(key, rid);
    }
  }


  private void updateBitMapIndexIfExists(int column, Tuple columnarTuple, int position)
      throws IOException, FieldNumberOutOfBoundException, HFDiskMgrException, InvalidTupleSizeException, HFException, SpaceNotAvailableException, InvalidTypeException, InvalidSlotNumberException, HFBufMgrException, GetFileEntryException, ConstructPageException, PinPageException, BMBufMgrException, UnpinPageException, BMException, AddFileEntryException {

    List<ColumnarHeaderRecord> bitMapFiles = bitmapIndexes.get(column);
    if (bitMapFiles != null) {
      ValueClass valueClass = Util.valueClassFactory(type[column - 1]);
      valueClass.setValueFromColumnTuple(columnarTuple, 1);

      ColumnarHeaderRecord record = null;
      for (ColumnarHeaderRecord infoRecord : bitMapFiles) {
        if (valueClass.equals(infoRecord)) {
          record = infoRecord;
          break;
        }
      }

      if (record == null) {
        //Create a new BitMap file for that particular value
        createBitMapIndex(column, valueClass);
      } else {
        // Update the position for the value
        //TODO: Check whether the functionality will work
        BitMapFile file = new BitMapFile(getBitMapFileName(column, valueClass));
        file.insert(position);
        try {
          file.close();
        }
        catch (Exception ex)
        {
          ex.printStackTrace();
        }
      }
    }
  }


  private String getBitMapFileName(int columnNo, ValueClass value) {
    return "BM_" + value.toString() + "_" + fileName + "." + columnNo;
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
    return new TupleScan(this);
  }

  /**
   * Initiate a sequential scan along a given column.
   */
  public Scan openColumnScan(int columnNo)
      throws InvalidTupleSizeException, IOException, HFDiskMgrException, HFBufMgrException, HFException {
    return new Scan(columnFiles[columnNo-1]);
  }

  /**
   * Updates the specified record in the columnar file.
   */
  public boolean updateTuple(TID tid, Tuple newtuple) throws Exception {

    for (int field = 1; field <= numColumns; field++) {
      boolean status = updateColumnofTuple(tid, newtuple, field);
      if (!status) {
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
    Tuple columnTuple = Util.createColumnarTuple(newtuple, column, type[column - 1]);
    return columnFiles[column - 1].updateRecord(tid.getRID(column - 1), columnTuple);
  }


  private ValueClass getDummyValue(AttrType attrType) {
    ValueClass value = null;
    switch (attrType.attrType) {
      case AttrType.attrInteger:
        value = new IntegerValue();
        value.setValue(1);
        break;
      case AttrType.attrString:
        value = new StringValue();
        value.setValue("");
        break;
      case AttrType.attrReal:
        value = new FloatValue();
        value.setValue(1.0);
        break;
    }
    return value;
  }

  private void insertHeaderInfoRecord(FileType fileType, int columnNo, String fileName,
      ValueClass value)
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException, FieldNumberOutOfBoundException, InvalidTypeException {
    headerFile = new Heapfile(infoHeaderFileName);

    int maxSize = 0;
    if(value instanceof StringValue)
    {
      maxSize = value.getValue().toString().length();
    }

    ColumnarHeaderRecord infoRecord = new ColumnarHeaderRecord(fileType, columnNo,
        type[columnNo - 1],
        fileName, value, maxSize);

    Tuple dfileTuple = infoRecord.getTuple();
    headerFile.insertRecord(dfileTuple.getTupleByteArray());

    //Also add the info the in memory map
    switch (infoRecord.getFileType()) {
      case BTREE_FILE:
        bTreeIndexes.put(infoRecord.getColumnNo(), infoRecord.getFileName());
        break;
      case BITMAP_FILE:
        if (bitmapIndexes.get(infoRecord.getColumnNo()) == null) {
          bitmapIndexes.put(infoRecord.getColumnNo(), new ArrayList<>());
        }
        bitmapIndexes.get(infoRecord.getColumnNo()).add(infoRecord);
        break;
    }
  }

  private String getBtreeFileName(int column) {
    return "BTree" + fileName + column;
  }

  /**
   * if it doesn’t exist, create a BTree index for the given column
   */
  public boolean createBTreeIndex(int column)
      throws Exception {

    // Check all the files it contains

    //TODO: Modify delete fashion if necessary
    int keySize = getKeySize(column);
    String bTreeFileName = getBtreeFileName(column);

    insertHeaderInfoRecord(FileType.BTREE_FILE, column, bTreeFileName, getDummyValue(type[column-1]));

    BTreeFile btf = new BTreeFile(bTreeFileName, type[column - 1].attrType,
        keySize, 1);//full delete

    Scan scan = new Scan(columnFiles[column - 1]);
    RID rid = new RID();

    Tuple temp = null;

    ValueClass valueClass = Util.valueClassFactory(type[column - 1]);
    int pos = 0;
    try {
      temp = scan.getNext(rid);
    } catch (Exception e) {
      e.printStackTrace();
    }

    while (temp != null) {
      // Copy to another variable so that the fields of the tuple are initialized.

      rid.position = pos++;
      Tuple t = new Tuple(temp.getTupleByteArray());
      t.tupleCopy(temp);
      KeyClass key = valueClass.getKeyClassFromColumnTuple(t, 1);
      btf.insert(key, rid);
      //System.out.println(key +" " +pos);

      temp = scan.getNext(rid);
    }

    btf.close();

    scan.closescan();

    return true;
  }

  int getKeySize(int column) {
    int strPtr = 0;
    for (int i = 0; i < column - 1; i++) {
      if (type[i].attrType == AttrType.attrString) {
        strPtr++;
      }
    }

    AttrType attrType = type[column - 1];
    int keySize = 0;

    switch (attrType.attrType) {
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
  public boolean createBitMapIndex(int columnNo, ValueClass value)
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidTypeException, SpaceNotAvailableException, InvalidSlotNumberException, GetFileEntryException, ConstructPageException, PinPageException, BMBufMgrException, UnpinPageException, BMException, AddFileEntryException {

    //Store the BitMap index file name in the header info heap
    //TODO: Uncomment it later
    insertHeaderInfoRecord(FileType.BITMAP_FILE, columnNo, getBitMapFileName(columnNo, value),value);

    // Create new BitMapFile
    BitMapFile file = new BitMapFile(getBitMapFileName(columnNo, value), this, columnNo, value);

    //file.printBitMap(columnFiles[columnNo-1]);

    return true;
  }

  public boolean createBitMapIndex(int columnNo)
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTypeException, GetFileEntryException, ConstructPageException, PinPageException, BMBufMgrException, UnpinPageException, BMException, AddFileEntryException {

    Set<ValueClass> uniqueValues = new HashSet();
    Heapfile columnHeapFile = new Heapfile(heapFileNames[columnNo - 1]);

    Scan scan = new Scan(columnHeapFile);
    RID rid = new RID();

    Tuple temp = null;

    try {
      temp = scan.getNext(rid);
    } catch (Exception e) {
      System.out.println("Exception in createBitMapIndex()...");
      e.printStackTrace();

      return false;
    }

    while (temp != null) {
      // Copy to another variable so that the fields of the tuple are initialized.

      Tuple tuple = new Tuple(temp.getTupleByteArray());
      tuple.tupleCopy(temp);
      ValueClass value = Util.valueClassFactory(type[columnNo - 1]);
      value.setValueFromColumnTuple(tuple, 1);

      //Create bitmap index for a unique value only once.
      if (!uniqueValues.contains(value)) {
        createBitMapIndex(columnNo, value);
        uniqueValues.add(value);
      }
      temp = scan.getNext(rid);
    }

    scan.closescan();

    return false;
  }

  /**
   * add the tuple to a heapfile tracking the deleted tuples from the columnar file
   */
  public boolean markTupleDeleted(int position)
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, InvalidSlotNumberException {

    Heapfile deleteFile = new Heapfile(getDeleteFileName());

    Tuple tuple = new Tuple();
    AttrType[] type = new AttrType[1];
    type[0] = new AttrType(AttrType.attrInteger);

    tuple.setHdr((short) 1, type, new short[0]);
    tuple.setIntFld(1, position);
    deleteFile.insertRecord(tuple.getTupleByteArray());

    return true;
  }

  /**
   * merge all deleted tuples from the file as well as all from all index files.
   */
  public boolean purgeAllDeletedTuples()
      throws Exception {

    Heapfile deleteFile = new Heapfile(getDeleteFileName());
    int numColumns = this.getNumColumns();
    ArrayList<TID> tidArrayList = new ArrayList<TID>();
    Scan scan = new Scan(deleteFile);
    RID rid = new RID();
    ArrayList<Integer> uniquePositions = new ArrayList<Integer>();

    Tuple temp = null;

    try {
      temp = scan.getNext(rid);
    } catch (Exception e) {
      e.printStackTrace();
    }
    //Open all the heap files and then scan the records position and jump to the desired position
    // find the RID corresponding to a given position and then delete the record by the RID
    try {
      while (temp != null) {
        Tuple t = new Tuple(temp.getTupleByteArray());
        t.tupleCopy(temp);
        int position = t.getIntFld(1);
        if (!uniquePositions.contains(position)) {
          uniquePositions.add(position);
          RID[] records = new RID[numColumns];
          for (int j = 0; j < numColumns; j++) {
            records[j] = columnar.Util.getRIDFromPosition(position - 1, columnFiles[j]);
          }
          TID tid = new TID(numColumns, position, records);
          tidArrayList.add(tid);
        }
        temp = scan.getNext(rid);
      }
    }catch (Exception e) {
      e.printStackTrace();
    }
    for(TID tid : tidArrayList){
      for(int j=0;j<numColumns;j++){
        columnFiles[j].deleteRecord(tid.getRID(j));
      }
    }
    scan = new Scan(deleteFile);
    rid = new RID();
    temp = null;
    boolean done = false;

    while (!done) {
      try {
        temp = scan.getNext(rid);
        if (temp == null || deleteFile.getRecCnt()==1) {
          done = true;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      try {
        deleteFile.deleteRecord(rid);
      } catch (Exception e) {
        System.err.println("*** Error deleting position from delete heap file ");
        e.printStackTrace();
        break;
      }
    }
    scan.closescan();
      return true;
  }
}
