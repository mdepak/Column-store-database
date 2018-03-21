package columnar;

import btree.KeyClass;
import global.AttrType;
import global.RID;
import global.TID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TupleScan {

  Columnarfile columnarfile;
  Scan[] scans;

  public TupleScan(Columnarfile cf) {
    int columns = cf.getNumColumns();
    Heapfile[] columnFiles = cf.getColumnFiles();
    Scan[] scans = new Scan[columns];
    for(int i=0; i<columns; i++)
      try {
        scans[i] = new Scan(columnFiles[i]);
      } catch (Exception e) {
        e.printStackTrace();
      }
      this.scans = scans;
      this.columnarfile = cf;
  }

  /**
   * Closes the TupleScan object
   */
  void closetuplescan() {

  }

  /**
   * Retrieve the next tuple in a sequential scan
   */
  public Tuple getNext(TID tid) {

    List<Integer> stringSizes = new ArrayList<>();
    int numberOfColumns = tid.getNumRIDs();
    Tuple temp = null;
    AttrType[] attrTypes = columnarfile.getType();
    ValueClass[] values = new ValueClass[numberOfColumns];
    for(int i=0; i<numberOfColumns; i++){
      try {
        Scan scan = scans[i];
        RID rid = tid.getRID(i);
        temp = scan.getNext(rid);
        if(temp == null){
          return null;
        }
        tid.setRID(i, rid);
        temp.initHeaders();
        ValueClass value = Util.valueClassFactory(attrTypes[i]);
        value.setValueFromColumnTuple(temp, 1);
        values[i] = value;
        System.out.println(value.getValue());
        if (value instanceof StringValue) {
          stringSizes.add(((String) value.getValue()).length());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    Tuple rowTuple = new Tuple();
    short[] strSizes = new short[stringSizes.size()];
    for (int idx = 0; idx < stringSizes.size(); idx++) {
      strSizes[idx] = (short) stringSizes.get(idx).intValue();
    }

    try {
      rowTuple.setHdr((short) numberOfColumns, attrTypes, strSizes);
      int fieldNo = 1;
      for (ValueClass value : values) {
        value.setValueinRowTuple(rowTuple, fieldNo++);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return rowTuple;
  }

  /**
   * Position all scan cursors to the records with the given rids
   */
  boolean position(TID tid) {
    return false;
  }

}
