package columnar;

import global.AttrType;
import global.TID;
import heap.Scan;
import heap.Tuple;

public class Columnarfile {

  //Field Summary
  static int numColumns;
  AttrType[] type;


  /**
   * Initialize: if columnar file does not exits, create one
   heapfile (‘‘name.columnid’’) per column; also create a
   ‘‘name.hdr’’ file that contains relevant metadata.
   * @param name
   * @param numColumns
   * @param type
   */
  Columnarfile(java.lang.String name, int numColumns, AttrType[] type)
  {
    //TODO: Create heap files according - implement it
  }

  /**
   * Delete all relevant files from the database.
   */
  void deleteColumnarFile()
  {

  }

  /**
   * Insert tuple into file, return its tid
   * @param tuplePtr
   * @return
   */
  TID insertTuple(byte[] tuplePtr)
  {
    return null;
  }

  /**
   * Read the tuple with the given tid from the columnar file
   * @param tid
   * @return
   */
  Tuple getTuple(TID tid)
  {
    return null;
  }

  /**
   * Read the value with the given column and tid from the
   columnar file
   * @param tid
   * @return
   */
  ValueClass getValue(TID tid, int column)
  {
    return null;
  }

  /**
   * Return the number of tuples in the columnar file.
   *
   * @return
   */
  int getTupleCnt()
  {
    return 0;
  }

  /**
   * Initiate a sequential scan of tuples.
   * @return
   */
  TupleScan openTupleScan()
  {
    return null;
  }

  /**
   * Initiate a sequential scan along a given column.

   * @param columnNo
   * @return
   */
  Scan openColumnScan(int columnNo)
  {
    return null;
  }

  /**
   * Updates the specified record in the columnar file.
   * @param tid
   * @param newtuple
   * @return
   */
  boolean updateTuple(TID tid, Tuple newtuple)
  {
    return false;
  }

  /**
   * Updates the specified column of the specified record in the
   columnar file
   * @param tid
   * @param newtuple
   * @param column
   * @return
   */
  boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
  {
    return false;
  }

  /**
   * if it doesn’t exist, create a BTree index for the given column
   * @param column
   * @return
   */
  boolean createBTreeIndex(int column)
  {
    return false;
  }

  /**
   * if it doesn’t exist, create a bitmap index for the given column
   and value
   * @param columnNo
   * @param value
   * @return
   */
  boolean createBitMapIndex(int columnNo, ValueClass value)
  {
    return false;
  }

  /**
   * add the tuple to a heapfile tracking the deleted tuples from
   the columnar file
   * @param tid
   * @return
   */
  boolean markTupleDeleted(TID tid)
  {
    return false;
  }

  /**
   * merge all deleted tuples from the file as well as all from all
   index files.
   * @return
   */
  boolean purgeAllDeletedTuples()
  {
    return false;
  }
}
