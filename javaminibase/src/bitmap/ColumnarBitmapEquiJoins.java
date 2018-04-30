package bitmap;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.BitmapIterator;
import columnar.Columnarfile;
import columnar.Util;
import columnar.ValueClass;
import global.AttrType;
import global.RID;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnarBitmapEquiJoins {

  private List<BitmapJoinFilePairs> bitmapJoinFilePairsList;
  private List<BitmapJoinpairNestedLoopScans> bitmapJoinpairNestedLoopScans;

  BitmapScan outer;
  BitmapScan inner;

  BitmapIterator leftIterator;
  BitmapIterator rightIterator;

  BitMapFile outerBitmap = null;
  BitMapFile innerBitmap = null;

  int outerPos, innerPos;

  List<Integer> lpositions = new ArrayList();
  List<Integer> rpositions = new ArrayList();

  boolean done = false, get_from_outer;

    Boolean outer_tuple = null;
    Boolean inner_tuple = null;

  public ColumnarBitmapEquiJoins(
          java.lang.String leftColumnarFileName,
          //int leftJoinField,
          java.lang.String rightColumnarFileName,
          //int rightJoinField,
          CondExpr[] joinCondExprs,
          CondExpr[] leftConds,
          CondExpr[] rightConds,
          String targetFieldValues,
          //FldSpec[] proj_list,
          int n_out_flds)
          throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, GetFileEntryException, ConstructPageException, InvalidSlotNumberException, InvalidTypeException, PinPageException, BMBufMgrException, SpaceNotAvailableException, UnpinPageException, BMException, DeleteFileEntryException, AddFileEntryException {

    Columnarfile leftColumnarFile = new Columnarfile(leftColumnarFileName);
    Columnarfile rightColumnarFile = new Columnarfile(rightColumnarFileName);

    bitmapJoinFilePairsList = BitmapUtil
        .getBitmapJoinFilePairsForCondExpr(joinCondExprs, leftColumnarFile, rightColumnarFile);

    bitmapJoinpairNestedLoopScans = BitmapUtil
        .openBitmapNestedLoopJoinsForBitmapPairs(bitmapJoinFilePairsList);


    get_from_outer = true;

    //TODO: Use bitmap iterator for the filtering data based on the outer filter and the inner filter and save the results to the temp bitmap file
    int leftOutList[] = {};
    int rightOutList[] = {};
    leftIterator = new BitmapIterator(leftColumnarFileName,leftOutList,leftConds);
    rightIterator = new BitmapIterator(rightColumnarFileName,rightOutList,rightConds);



    Integer tuple = leftIterator.get_next_pos();
    while (tuple >=0) {
      lpositions.add(tuple);
      tuple = leftIterator.get_next_pos();
    }
    tuple = rightIterator.get_next_pos();
    while (tuple >=0) {
      rpositions.add(tuple);
      tuple = rightIterator.get_next_pos();
    }

    if (lpositions == null || lpositions.isEmpty()) {
      outerBitmap = new BitMapFile("lcon",leftColumnarFile.getColumnFiles()[0].getRecCnt(),true);
    }
    else {
      outerBitmap = new BitMapFile("lcon",leftColumnarFile.getColumnFiles()[0].getRecCnt(),false);
      for( int i : lpositions){
        outerBitmap.insertAtPosition(i);
        }
    }
    if (rpositions == null || rpositions.isEmpty()) {
      innerBitmap = new BitMapFile("rcon",rightColumnarFile.getColumnFiles()[0].getRecCnt(),true);
    }
    else {
      innerBitmap = new BitMapFile("rcon",rightColumnarFile.getColumnFiles()[0].getRecCnt(),false);
      for( int i : rpositions){
        innerBitmap.insertAtPosition(i);
      }
    }

    //TODO: Fix it after completing and writing proper method
    outer = new BitmapScan(outerBitmap);
    inner = new BitmapScan(innerBitmap);

    get_from_outer = true;

      //target columns extraction
      String outputTargetFieldValues = targetFieldValues.replaceAll("\\[", "").replaceAll("\\]","");
      String[] outputCoulmnsInOrder = outputTargetFieldValues.split(",");
      int numOfAttributesInResultTuple = outputCoulmnsInOrder.length;

      //building FldSpec for joined tuple & target column extraction for both the tables
      FldSpec[] perm_mat = new FldSpec[numOfAttributesInResultTuple];
      for(int i=0; i<numOfAttributesInResultTuple; i++) {
          String[] outputColumn = outputCoulmnsInOrder[i].split("\\.");
          if(outputColumn[0].equals(leftColumnarFileName)) {
              perm_mat[i] = new FldSpec(new RelSpec(0), ((int) outputColumn[1].charAt(0)) - 64);
          } else {
              perm_mat[i] = new FldSpec(new RelSpec(1), ((int) outputColumn[1].charAt(0)) - 64);
          }
      }
      FldSpec[] outerFldSpec = getFldSpec(true, leftColumnarFile);
      FldSpec[] innerFldSpec = getFldSpec(false, rightColumnarFile);

  }
    public FldSpec[] getFldSpec(boolean outer, Columnarfile cf) {
        int numOfAttributes = cf.getNumColumns();
        FldSpec[] fldSpec = new FldSpec[numOfAttributes];
        for(int i=0; i<numOfAttributes; i++) {
            if(outer) {
                fldSpec[i] = new FldSpec(new RelSpec(0), i+1);
            }else {
                fldSpec[i] = new FldSpec(new RelSpec(1), i+1);
            }
        }
        return fldSpec;
    }
  public Tuple getNext() throws NestedLoopException, InvalidTupleSizeException, IOException
    {

        RID o_rid = new RID();
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
                    innerPos = 0;
                }

                try {
                    inner = new BitmapScan(innerBitmap);
                } catch (Exception e) {
                    throw new NestedLoopException(e, "openScan failed");
                }

                if ((outer_tuple = outer.getNext(o_rid)) == null) {

                    done = true;
                    if (inner != null) {

                        inner = null;
                    }

                    return null;
                }
                else
                {
                    outerPos++;
                }
            }  // ENDS: if (get_from_outer == TRUE)

            RID i_rid = new RID();
            while ((inner_tuple = inner.getNext(i_rid)) != null) {
                List<BitmapCondExprValues> condExprValues = BitmapUtil
                        .getNextBitmapNestedLoopJoins(bitmapJoinpairNestedLoopScans, outer_tuple,
                                inner_tuple);

                Boolean result = BitmapUtil.evaluateBitmapCondExpr(condExprValues);

                if (result) {
                    System.out.println("Result at "+result+" Outer pos: " + outerPos + "\t inner pos: " + innerPos);
                    innerPos++;
                    return new Tuple();
                }
            }

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
