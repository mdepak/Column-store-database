package columnar;

import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import iterator.CondExpr;
import java.io.IOException;

public class BitmapIterator {


  Columnarfile columnarfile;

  ColumnarBitmapScan bitmapScan;

  public BitmapIterator(String columnarFile, int outputColumnsIndexes[],
      CondExpr selects[], final boolean indexOnly)
      throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException {

    //Choose appropriate number of bit map files based on the condition
    // Open the BitMapScan for the bitmap files and return value when all the scans match

    columnarfile = new Columnarfile(columnarFile);

    String bitMapFile = ""; //Choose bitmap file name based on the condition

    //TODO:Considering only 1 equality condition

  }

  public Tuple get_next(int position) {

    /*while(bitmapScan.get_next(position) != null)
    {

    }*/

    return null;
  }

  public void close() {
    bitmapScan.close();
  }
}
