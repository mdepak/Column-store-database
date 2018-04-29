package bitmap;

import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.Columnarfile;
import global.AttrType;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.NestedLoopException;
import java.io.IOException;
import java.util.List;

public class ColumnarBitmapEquiJoins {

  private List<BitmapJoinFilePairs> bitmapJoinFilePairsList;
  private List<BitmapJoinpairNestedLoopScans> bitmapJoinpairNestedLoopScans;

  BitmapScan outer;
  BitmapScan inner;


  BitMapFile outerBitmap = null;
  BitMapFile innerBitmap = null;

  int outerPos, innerPos;

  boolean done = false, get_from_outer;

  public ColumnarBitmapEquiJoins(
      AttrType[] in1,
      int len_in1,
      short[] t1_str_sizes,
      AttrType[] in2,
      int len_in2,
      short[] t2_str_sizes,
      int amt_of_mem,
      java.lang.String leftColumnarFileName,
      int leftJoinField,
      java.lang.String rightColumnarFileName,
      int rightJoinField,
      FldSpec[] proj_list,
      int n_out_flds, CondExpr[] joinCondExprs)

      throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, GetFileEntryException, ConstructPageException {

    Columnarfile leftColumnarFile = new Columnarfile(leftColumnarFileName);
    Columnarfile rightColumnarFile = new Columnarfile(rightColumnarFileName);

    bitmapJoinFilePairsList = BitmapUtil
        .getBitmapJoinFilePairsForCondExpr(joinCondExprs, leftColumnarFile, rightColumnarFile);

    bitmapJoinpairNestedLoopScans = BitmapUtil
        .openBitmapNestedLoopJoinsForBitmapPairs(bitmapJoinFilePairsList);


    get_from_outer = true;

    //TODO: Use bitmap iterator for the filtering data based on the outer filter and the inner filter and save the results to the temp bitmap file

    outerBitmap = null;
    innerBitmap = null;

    outer = new BitmapScan(outerBitmap);
    inner = new BitmapScan(innerBitmap);
  }

  public Tuple getNext() throws NestedLoopException, InvalidTupleSizeException, IOException {
    Boolean outer_tuple = null;
    Boolean inner_tuple = null;

    if (done) {
      return null;
    }

    do {
      // If get_from_outer is true, Get a tuple from the outer, delete
      // an existing scan on the file, and reopen a new scan on the file.
      // If a get_next on the outer returns DONE?, then the nested loops
      //join is done too.

      if (get_from_outer == true) {
        get_from_outer = false;
        if (inner != null)     // If this not the first time,
        {
          // close scan
          inner = null;
        }

        try {
          inner = new BitmapScan(innerBitmap);
          innerPos = 0;
        } catch (Exception e) {
          throw new NestedLoopException(e, "Inner nested bit map  scan openScan failed");
        }

        while ((outer_tuple = outer.getNext(new RID())) == false) {
          outerPos++;
        }

        if ((outer_tuple = outer.getNext(new RID())) == null) {
          done = true;
          if (inner != null) {

            inner = null;
          }

          return null;
        }
      }  // ENDS: if (get_from_outer == TRUE)

      // The next step is to get a tuple from the inner,
      // while the inner is not completely scanned && there
      // is no match (with pred),get a tuple from the inner.

      RID rid = new RID();
      while ((inner_tuple = inner.getNext(rid)) != null) {
        innerPos++;
        if (inner_tuple) {
          //Construct the tuple and return the data

          List<BitmapCondExprValues> condExprValues = BitmapUtil
              .getNextBitmapNestedLoopJoins(bitmapJoinpairNestedLoopScans, outer_tuple,
                  inner_tuple);

          Boolean result = BitmapUtil.evaluateBitmapCondExpr(condExprValues);

          if (result) {
            System.out.println("Outer pos:" + outerPos + "\t inner pos:" + innerPos);
            return new Tuple();
          }
        }
      }

      // There has been no match. (otherwise, we would have
      //returned from t//he while loop. Hence, inner is
      //exhausted, => set get_from_outer = TRUE, go to top of loop

      get_from_outer = true; // Loop back to top and get next outer tuple.
    } while (true);

  }


  public void close()
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
    outer.closescan();
    inner.closescan();
    BitmapUtil.closeBitmapJoinPairNestedLoopScans(bitmapJoinpairNestedLoopScans);
  }
}
