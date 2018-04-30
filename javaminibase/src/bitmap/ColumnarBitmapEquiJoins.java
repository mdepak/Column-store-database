package bitmap;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.DeleteFileEntryException;
import btree.GetFileEntryException;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.BitmapIterator;
import columnar.Columnarfile;
import columnar.Util;
import global.AttrType;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.NestedLoopException;
import iterator.RelSpec;
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


  FldSpec[] outerFldSpec;
  FldSpec[] innerFldSpec;

  Columnarfile leftColumnarFile;
  Columnarfile rightColumnarFile;


  boolean done = false, get_from_outer;

  Boolean outer_tuple = null;
  Boolean inner_tuple = null;

  public ColumnarBitmapEquiJoins(
          String leftColumnarFileName,
      //int leftJoinField,
          String rightColumnarFileName,
      //int rightJoinField,
      CondExpr[] joinCondExprs,
      CondExpr[] leftConds,
      CondExpr[] rightConds,
      String targetFieldValues,
      //FldSpec[] proj_list,
      int numBuf)
      throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, GetFileEntryException, ConstructPageException, InvalidSlotNumberException, InvalidTypeException, PinPageException, BMBufMgrException, SpaceNotAvailableException, UnpinPageException, BMException, DeleteFileEntryException, AddFileEntryException {

    leftColumnarFile = new Columnarfile(leftColumnarFileName);
    rightColumnarFile = new Columnarfile(rightColumnarFileName);

    bitmapJoinFilePairsList = BitmapUtil
        .getBitmapJoinFilePairsForCondExpr(joinCondExprs, leftColumnarFile, rightColumnarFile);

    bitmapJoinpairNestedLoopScans = BitmapUtil
        .openBitmapNestedLoopJoinsForBitmapPairs(bitmapJoinFilePairsList);

    get_from_outer = true;

    //TODO: Use bitmap iterator for the filtering data based on the outer filter and the inner filter and save the results to the temp bitmap file
    int leftOutList[] = {};
    int rightOutList[] = {};
    leftIterator = new BitmapIterator(leftColumnarFileName, leftOutList, leftConds);
    rightIterator = new BitmapIterator(rightColumnarFileName, rightOutList, rightConds);

    Integer tuple = leftIterator.get_next_pos();
    while (tuple >= 0) {
      lpositions.add(tuple);
      tuple = leftIterator.get_next_pos();
    }
    tuple = rightIterator.get_next_pos();
    while (tuple >= 0) {
      rpositions.add(tuple);
      tuple = rightIterator.get_next_pos();
    }

    if (lpositions == null || lpositions.isEmpty()) {
      outerBitmap = new BitMapFile("lcon", leftColumnarFile.getColumnFiles()[0].getRecCnt(), true);
    } else {
      outerBitmap = new BitMapFile("lcon", leftColumnarFile.getColumnFiles()[0].getRecCnt(), false);
      for (int i : lpositions) {
        outerBitmap.insertAtPosition(i);
      }
    }
    if (rpositions == null || rpositions.isEmpty()) {
      innerBitmap = new BitMapFile("rcon", rightColumnarFile.getColumnFiles()[0].getRecCnt(), true);
    } else {
      innerBitmap = new BitMapFile("rcon", rightColumnarFile.getColumnFiles()[0].getRecCnt(),
          false);
      for (int i : rpositions) {
        innerBitmap.insertAtPosition(i);
      }
    }

    //TODO: Fix it after completing and writing proper method
    outer = new BitmapScan(outerBitmap);
    inner = new BitmapScan(innerBitmap);

    get_from_outer = true;

    //target columns extraction
    String outputTargetFieldValues = targetFieldValues.replaceAll("\\[", "").replaceAll("\\]", "");
    String[] outputCoulmnsInOrder = outputTargetFieldValues.split(",");
    int numOfAttributesInResultTuple = outputCoulmnsInOrder.length;

    //building FldSpec for joined tuple & target column extraction for both the tables
    FldSpec[] perm_mat = new FldSpec[numOfAttributesInResultTuple];
    int leftProjCount = 0;
    int rightProjCount = 0;
    for (int i = 0; i < numOfAttributesInResultTuple; i++) {
      String[] outputColumn = outputCoulmnsInOrder[i].split("\\.");
      if (outputColumn[0].equals(leftColumnarFileName)) {
        leftProjCount+=1;
      } else {
        rightProjCount+=1;
      }
    }
    outerFldSpec = new FldSpec[leftProjCount];
    innerFldSpec = new FldSpec[rightProjCount];
    int leftIterator = 0;
    int rightIterator = 0;
    for (int i = 0; i < numOfAttributesInResultTuple; i++) {
      String[] outputColumn = outputCoulmnsInOrder[i].split("\\.");
      if (outputColumn[0].equals(leftColumnarFileName)) {
        outerFldSpec[leftIterator++] = new FldSpec(new RelSpec(0), ((int) outputColumn[1].charAt(0)) - 64);
      } else {
        innerFldSpec[rightIterator++] = new FldSpec(new RelSpec(1), ((int) outputColumn[1].charAt(0)) - 64);
      }
    }

  }


  public FldSpec[] getFldSpec(boolean outer, Columnarfile cf) {
    int numOfAttributes = cf.getNumColumns();
    FldSpec[] fldSpec = new FldSpec[numOfAttributes];
    for (int i = 0; i < numOfAttributes; i++) {
      if (outer) {
        fldSpec[i] = new FldSpec(new RelSpec(0), i + 1);
      } else {
        fldSpec[i] = new FldSpec(new RelSpec(1), i + 1);
      }
    }
    return fldSpec;
  }

  public Tuple getNext() throws NestedLoopException, InvalidTupleSizeException, IOException {

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
//          if (inner != null) {
//            innerPos = 0;
//            inner = null;
//          }

          return null;
        }
        //else {
         // outerPos++;
        //}
      }  // ENDS: if (get_from_outer == TRUE)

      RID i_rid = new RID();
      while ((inner_tuple = inner.getNext(i_rid)) != null) {
        List<BitmapCondExprValues> condExprValues = BitmapUtil
            .getNextBitmapNestedLoopJoins(bitmapJoinpairNestedLoopScans, outer_tuple,
                inner_tuple);

        Boolean result = BitmapUtil.evaluateBitmapCondExpr(condExprValues);
//        System.out.println(
//                "Result = " + result + " Outer pos: " + outerPos + "\t inner pos: " + innerPos);

        if (result) {

          try {
            printTupleAtPosition(outerPos, outerFldSpec, leftColumnarFile);
            printTupleAtPosition(innerPos, innerFldSpec, rightColumnarFile);
            System.out.println();
          } catch (HFBufMgrException | InvalidSlotNumberException | FieldNumberOutOfBoundException e) {
            System.out.println("Exception in printing equi join tuple");
            e.printStackTrace();
          }
        }
        innerPos++;
      }

      get_from_outer = true; // Loop back to top and get next outer tuple.
      outerPos++;
    } while (true);
  }


  private void printTupleAtPosition(int position, FldSpec[] fldSpecs, Columnarfile file)
      throws InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException, FieldNumberOutOfBoundException {
    AttrType[] attrTypes = file.getType();
    Heapfile[] files = file.getColumnFiles();

    for (int i = 0; i < fldSpecs.length; i++) {
      int colIndex = fldSpecs[i].offset-1;

      AttrType attrType = attrTypes[colIndex];
      Tuple tuple = Util.getTupleFromPosition(position, files[colIndex]);
      tuple.initHeaders();
      switch (attrType.attrType) {
        case AttrType.attrInteger:
          System.out.print(tuple.getIntFld(1) + "\t");
          break;
        case AttrType.attrString:
          System.out.print(tuple.getStrFld(1) + "\t");
          break;
      }
    }
  }

  public void close()
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
    outer.closescan();
    inner.closescan();
    BitmapUtil.closeBitmapJoinPairNestedLoopScans(bitmapJoinpairNestedLoopScans);
  }
}
