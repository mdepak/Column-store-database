package columnar;

import static global.AttrType.attrInteger;
import static global.AttrType.attrNull;
import static global.AttrType.attrReal;
import static global.AttrType.attrString;
import static global.AttrType.attrSymbol;

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


class ColumnarTupleConvertor {

  public static Tuple createColumnarTuple(Tuple rowTuple, int fieldNo, AttrType attrType)
      throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException {
    Tuple columnTuple = new Tuple();
    short colTupFields = 1;
    switch (attrType.attrType) {
      case attrString:
        String strVal = rowTuple.getStrFld(fieldNo);
        columnTuple
            .setHdr(colTupFields, new AttrType[]{attrType}, new short[]{(short) strVal.length()});
        columnTuple.setStrFld(1, strVal);
        break;
      case attrInteger:
        int intVal = rowTuple.getIntFld(fieldNo);
        columnTuple.setHdr(colTupFields, new AttrType[]{attrType}, new short[]{});
        columnTuple.setIntFld(1, intVal);
        break;
      case attrReal:
        float floatVal = rowTuple.getFloFld(fieldNo);
        columnTuple.setHdr(colTupFields, new AttrType[]{attrType}, new short[]{});
        columnTuple.setFloFld(1, floatVal);
        break;
      case attrSymbol:
        //TODO: Find out whether this is character and implement it.
        break;
      case attrNull:
        //TODO: Find out the right type and implement it.
        break;
    }

    return columnTuple;
  }
}


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
      Tuple columnTuple = ColumnarTupleConvertor.createColumnarTuple(rowTuple, i + 1, type[i]);
      resultRIDs[i] = columnFiles[i].insertRecord(columnTuple.getTupleByteArray());
    }

    return new TID(numColumns, position, resultRIDs);
  }

  /**
   * Read the tuple with the given tid from the columnar file
   */
  Tuple getTuple(TID tid) throws Exception {

    Tuple[] rowTuples = new Tuple[numColumns];

    //Get the tuples from each columbar heap file
    for (int field = 0; field < numColumns; field++) {
      rowTuples[field] = columnFiles[field].getRecord(tid.getRID(field));
    }

    // Merge all the columns values and form the row tuple

    return null;
  }

  /**
   * Read the value with the given column and tid from the columnar file
   */
  ValueClass getValue(TID tid, int column) {
    return null;
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
  boolean updateTuple(TID tid, Tuple newtuple) {
    return false;
  }

  /**
   * Updates the specified column of the specified record in the columnar file
   */
  boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) {
    return false;
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
