package columnar;

import bitmap.BitMapFile;
import bitmap.BitmapCondExprScanFiles;
import bitmap.BitmapCondExprScans;
import bitmap.BitmapCondExprValues;
import bitmap.BitmapUtil;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import iterator.CondExpr;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BitmapIterator {

  private Columnarfile columnarfile;

  private AttrType[] reqAttrType;

  private short[] strSizes;

  private BitMapFile bitMapFile;


  //TODO: Instead of this way rather try to maintain a map for file to ScanObject mapping and then choose the approprite File
  // This approach will reduce the same bitmap file being used by multiple conditions
  private List<BitmapCondExprScanFiles> condExprScanFiles;

  private List<BitmapCondExprScans> condExprScans;

  private int[] projCols;

  // Variable for holding the position of the tuple order - to be used for the retrieval of tuples
  private int position;

  public BitmapIterator(String relationName, int indexField, int outputColumnsIndexes[],
      CondExpr selects[], final boolean indexOnly)
      throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, GetFileEntryException, ConstructPageException {

    columnarfile = new Columnarfile(relationName);

    Columnarfile columnarFile = new Columnarfile(relationName);
    AttrType[] attrType = columnarFile.getType();
    int numOfOutputColumns = outputColumnsIndexes.length;
    reqAttrType = new AttrType[numOfOutputColumns];

    short[] str_sizes = new short[numOfOutputColumns];

    short[] s_sizes = columnarFile.getStrSizes();
    int j = 0;

    for (int i = 0; i < numOfOutputColumns; i++) {
      reqAttrType[i] = attrType[outputColumnsIndexes[i] - 1];
      if (reqAttrType[i].attrType == AttrType.attrString) {
        str_sizes[j] = s_sizes[outputColumnsIndexes[i] - 1];
        j++;
      }
    }

    projCols = outputColumnsIndexes;

    strSizes = Arrays.copyOfRange(str_sizes, 0, j);

    //Choose appropriate number of bit map files based on the condition
    // Open the BitMapScan for the bitmap files and return value when all the scans match

    condExprScanFiles = BitmapUtil.getBitmapScanForCondExpr(selects, columnarfile);
    condExprScans = BitmapUtil.constructBitmapScanFromBitmapFiles(condExprScanFiles);
  }


  public Integer get_next()
      throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, HFBufMgrException, InvalidSlotNumberException {

    List<BitmapCondExprValues> condExprValues;

    while (true) {

      if ((condExprValues = BitmapUtil.getNext(condExprScans)) == null) {
        return null;
      }
      if (BitmapUtil.evaluateBitmapCondExpr(condExprValues)) {
        return position++;
      }

      position++;
    }
  }

  public void close()
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

    //TODO: Write close function for the list objects and close them.
  }
}
