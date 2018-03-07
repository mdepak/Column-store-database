package columnar;

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

  private String fileName;
  private Heapfile[] columnFiles;
  private String[] heapFileNames;


  /**
   * Initialize: if columnar file does not exits, create one heapfile (‘‘name.columnid’’) per
   * column; also create a ‘‘name.hdr’’ file that contains relevant metadata.
   */
  public Columnarfile(java.lang.String name, int numColumns, AttrType[] type)
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
    //TODO: Create heap files according - implement it

    if (name != null) {
      fileName = name;
      this.numColumns = numColumns;
      this.type = type;
    }

    heapFileNames = new String[numColumns];
    columnFiles = new Heapfile[numColumns];

    for (int i = 0; i < numColumns; i++) {
      heapFileNames[i] = fileName + "." + (i + 1);
      columnFiles[i] = new Heapfile(heapFileNames[i]);
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
  boolean createBTreeIndex(int column) {
    return false;
  }

  /**
   * if it doesn’t exist, create a bitmap index for the given column and value
   */
  boolean createBitMapIndex(int columnNo, ValueClass value) {
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
