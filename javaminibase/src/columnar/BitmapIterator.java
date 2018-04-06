package columnar;

import bitmap.BitMapFile;
import bitmap.BitmapScan;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrType;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.CondExpr;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BitmapIterator {

  private Columnarfile columnarfile;

  private BitmapScan bitmapScan;

  private AttrType[] reqAttrType;

  private short[] strSizes;

  private BitMapFile bitMapFile;

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

    //TODO:Considering only 1 equality condition - extend for multiple conditions and different attributes
    CondExpr condition = selects[0];
    ValueClass operandValue = Util.valueClassFactory(condition.type2);
    operandValue.setValue(condition.operand2.getOperandValue(condition.type2));
    /*if (condition.op.attrOperator == AttrOperator.aopEQ) {

    }*/

    List<ColumnarHeaderRecord> list = columnarFile.getBitMapIndicesInfo(indexField);

    String bitMapFileName = null; //Choose bitmap file name based on the condition

    for (ColumnarHeaderRecord headerRecord : list) {
      if (operandValue.equals(headerRecord.getValueClass())) {
        bitMapFileName = headerRecord.getFileName();
        break;
      }
    }

    bitMapFile = new BitMapFile(bitMapFileName);
    bitmapScan = new BitmapScan(bitMapFile);
  }


  public Tuple get_next()
      throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, HFBufMgrException, InvalidSlotNumberException {

    RID rid = new RID();

    Boolean bit;

    while (true) {
      if ((bit = bitmapScan.getNext(rid)) == null) {
        return null;
      }

      //TODO: Instead of checking only one condition check multiple conditions - write a ProjEval kind of for BitMap
      // for combining multiple bitmap file results
      if (bit) {
        //Construct the tuple and return the data
        return Util
            .getRowTupleFromPosition(position++, columnarfile, projCols, reqAttrType, strSizes);

      }
      position++;
    }
  }

  public void close()
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
    bitmapScan.closescan();
    bitMapFile.close();
  }
}
