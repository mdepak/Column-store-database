package iterator;

import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import columnar.Util;
import global.AttrType;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnarSort extends Iterator {

  private FileScan fscan;
  private Sort sort;
  private Columnarfile columnarfile;


  private AttrType[] projectionAttrs;
  private short[] projectionStrSizes;
  int[] selectedCols;

  public ColumnarSort(
      Columnarfile columnarfile,
      int[] selectedCols,
      int sort_fld,
      TupleOrder sort_order,
      int n_pages)
      throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, FileScanException, TupleUtilsException, InvalidRelation, SortException {

    try {
      this.columnarfile = columnarfile;

      Heapfile sortColHeapFile = columnarfile.getColumnFiles()[sort_fld - 1];

      AttrType[] colAttrTypes = columnarfile.getType();
      AttrType[] sortAttrTypes = new AttrType[1];
      sortAttrTypes[0] = colAttrTypes[sort_fld - 1];

      short[] strSizes = columnarfile.getStrSizes();

      //this.selectedCols = selectedCols;

      this.selectedCols = new int[colAttrTypes.length];
      for (int i = 0; i < colAttrTypes.length; i++) {
        this.selectedCols[i] = i + 1;
      }

      //setProjectionAttrs(colAttrTypes);
      //setProjectionStrSizes(colAttrTypes, strSizes);

      projectionAttrs = colAttrTypes;
      projectionStrSizes = strSizes;

      short[] sortStrSizes;
      if (sortAttrTypes[0].attrType == AttrType.attrString) {
        sortStrSizes = new short[1];
        int strPtr = 0;

        for (int idx = 0; idx < colAttrTypes.length; idx++) {

          if (idx == sort_fld - 1) {
            sortStrSizes[0] = strSizes[strPtr];
          }

          if (AttrType.attrString == colAttrTypes[idx].attrType) {
            strPtr++;
          }
        }
      } else {
        sortStrSizes = new short[0];
      }

      FldSpec[] projlist = new FldSpec[1];
      RelSpec rel = new RelSpec(RelSpec.outer);
      projlist[0] = new FldSpec(rel, 1);

      fscan = new FileScan(sortColHeapFile._fileName, sortAttrTypes, sortStrSizes, (short) 1, 1,
          projlist, null);

      short sortFieldLen = 0;

      switch (sortAttrTypes[0].attrType) {
        case AttrType.attrInteger:
          sortFieldLen = 4;
          break;
        case AttrType.attrString:
          sortFieldLen = sortStrSizes[0];
          break;
      }
      sort = new Sort(sortAttrTypes, (short) 1, sortStrSizes, fscan, 1, sort_order, sortFieldLen,
          3, true);
    }
  catch (RuntimeException ex) {
      ex.printStackTrace();

  }


  }


  private void setProjectionAttrs(AttrType[] columnAttrTypes) {
    projectionAttrs = new AttrType[selectedCols.length];

    for (int idx = 0; idx < projectionAttrs.length; idx++) {
      projectionAttrs[idx] = columnAttrTypes[selectedCols[idx] - 1];
    }
  }


  private void setProjectionStrSizes(AttrType[] columnAttrTypes, short[] columnStrSize) {
    List<Integer> strFields = new ArrayList();

    int selFldsPtr = 0;
    int strSizePtr = 0;

    for (int i = 0; i < columnAttrTypes.length; i++) {
      if (i == selectedCols[selFldsPtr]) {
        if (columnAttrTypes[i].attrType == AttrType.attrString) {
          strFields.add((int) columnStrSize[strSizePtr++]);
          selFldsPtr++;
        }
      } else {
        if (columnAttrTypes[i].attrType == AttrType.attrString) {
          strSizePtr++;
        }
      }
    }

    projectionStrSizes = new short[strFields.size()];
    for (int pos = 0; pos < strFields.size(); pos++) {
      projectionStrSizes[pos] = (short) strFields.get(pos).intValue();
    }
  }

  @Override
  public Tuple get_next()
      throws Exception {

    try {
      Tuple tuple = sort.get_next();

      //End of the scan
      if (tuple == null) {
        return null;
      }

      int position = tuple.getIntFld(2);

      return Util.getRowTupleFromPosition(position, columnarfile, selectedCols, projectionAttrs,
          projectionStrSizes);
    }
    catch(Exception ex) {
      fscan.close();
      sort.close();
      throw ex;
    }
  }

  @Override
  public int get_next_pos()
      throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    return 0;
  }

  @Override
  public void close() throws Exception {
    if(sort!=null)
      sort.close();

    if(fscan !=null)
    fscan.close();
  }
}
