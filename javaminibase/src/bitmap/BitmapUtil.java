package bitmap;

import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.ColumnarHeaderRecord;
import columnar.Columnarfile;
import columnar.Util;
import columnar.ValueClass;
import global.AttrOperator;
import global.RID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import iterator.CondExpr;
import iterator.NestedLoopException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class BitmapUtil {


  private static Boolean evaluateOR(List<Boolean> booleanList) {
    Boolean result = false;
    for (Boolean value : booleanList) {
      result = result || value;
    }

    return result;
  }

  private static BiFunction<ValueClass, ValueClass, Boolean> getEvaluationFunction(
      AttrOperator operator) {
    //TODO: Add a switch case and return a function for each operator type.
    //TODO: Write a function in the Value class and call the functions appropriately.

    switch (operator.attrOperator) {
      case AttrOperator.aopEQ:
        return (ValueClass value1, ValueClass value2) -> value1.evaluateEquals(value2);
      case AttrOperator.aopLT:
        return (ValueClass value1, ValueClass value2) -> value1.evaluateLT(value2);
      case AttrOperator.aopGT:
        return (ValueClass value1, ValueClass value2) -> value1.evaluateGT(value2);
      case AttrOperator.aopNE:
        return (ValueClass value1, ValueClass value2) -> value1.evaluateNotEquals(value2);
      case AttrOperator.aopLE:
        return (ValueClass value1, ValueClass value2) -> value1.evaluateLTEquals(value2);
      case AttrOperator.aopGE:
        return (ValueClass value1, ValueClass value2) -> value1.evaluateGTEquals(value2);
      case AttrOperator.aopNOT:
        return (ValueClass value1, ValueClass value2) -> value1.evaluateNotEquals(value2);
    }

    return null;
  }

  private static List<String> getBitmapFileforCondExpr(CondExpr expression,
      Columnarfile columnarFile) {
    List<String> bitmapFileList = new ArrayList<>();

    ValueClass operandValue = Util.valueClassFactory(expression.type2);
    operandValue.setValue(expression.operand2.getOperandValue(expression.type2));

    int columnNum;
    if(expression.operand1.symbol ==null){

      columnNum = Integer.parseInt(expression.operand1.string);
    }
    else{
      columnNum = expression.operand1.symbol.offset;
    }
    List<ColumnarHeaderRecord> headerRecordList = columnarFile.getBitMapIndicesInfo(columnNum);

    BiFunction<ValueClass, ValueClass, Boolean> evalFunc = getEvaluationFunction(expression.op);

    for (ColumnarHeaderRecord headerRecord : headerRecordList) {
      if (evalFunc.apply(headerRecord.getValueClass(), operandValue)) {
        bitmapFileList.add(headerRecord.getFileName());
      }
    }

    return bitmapFileList;
  }

  public static List<BitmapCondExprScanFiles> getBitmapScanForCondExpr(CondExpr[] condExprs,
      Columnarfile columnarfile)
      throws HFDiskMgrException, HFException, IOException, ConstructPageException, GetFileEntryException, HFBufMgrException {
    List<BitmapCondExprScanFiles> condExprScans = new ArrayList<>();

    for (int i = 0; i < condExprs.length - 1; i++) {
      CondExpr expression = condExprs[i];
      BitmapCondExprScanFiles scanFiles = new BitmapCondExprScanFiles();

      condExprScans.add(scanFiles);

      //Current object in the list
      BitmapCondExprScanFiles currObj = scanFiles;
      while (expression != null) {

        List<String> bmFileNameList = getBitmapFileforCondExpr(expression, columnarfile);

        List<BitMapFile> bmFileList = new ArrayList();
        for (String bitmapFileName : bmFileNameList) {
          bmFileList.add(new BitMapFile(bitmapFileName));
        }

        // Set the opened bm files to the list
        currObj.setScanList(bmFileList);

        expression = expression.next;
        if (expression != null) {
          currObj.next = new BitmapCondExprScanFiles();
          currObj = currObj.next;
        }
      }
    }

    return condExprScans;
  }


  public static List<BitmapCondExprScans> constructBitmapScanFromBitmapFiles(
      List<BitmapCondExprScanFiles> scanFilesList)
      throws InvalidTupleSizeException, IOException {
    List<BitmapCondExprScans> condExprScansList = new ArrayList();

    for (int idx = 0; idx < scanFilesList.size(); idx++) {

      BitmapCondExprScans condExprScans = new BitmapCondExprScans();

      condExprScansList.add(condExprScans);

      BitmapCondExprScanFiles scanFile = scanFilesList.get(idx);
      //Current object in the list
      BitmapCondExprScans currObj = condExprScans;

      while (scanFile != null) {
        List<BitmapScan> scanList = new ArrayList<>();

        for (BitMapFile file : scanFile.getScanList()) {
          scanList.add(new BitmapScan(file));
        }

        currObj.setScanList(scanList);

        scanFile = scanFile.next;
        if (scanFile != null) {
          currObj.next = new BitmapCondExprScans();
          currObj = currObj.next;
        }
      }

    }

    return condExprScansList;

  }

  public static List<BitmapJoinpairNestedLoopScans> openBitmapNestedLoopJoinsForBitmapPairs(
      List<BitmapJoinFilePairs> bitmapJoinPairs)
      throws HFDiskMgrException, InvalidTupleSizeException, HFException, IOException, ConstructPageException, GetFileEntryException, HFBufMgrException {

    List<BitmapJoinpairNestedLoopScans> condExprScansList = new ArrayList();

    for (int idx = 0; idx < bitmapJoinPairs.size(); idx++) {

      BitmapJoinpairNestedLoopScans condExprScans = new BitmapJoinpairNestedLoopScans();

      condExprScansList.add(condExprScans);

      BitmapJoinFilePairs fileJoinPairs = bitmapJoinPairs.get(idx);
      //Current object in the list
      BitmapJoinpairNestedLoopScans currObj = condExprScans;

      while (fileJoinPairs != null) {
        List<BitmapNestedLoopScan> scanList = new ArrayList<>();

        for (BitmapPair joinPair : fileJoinPairs.getFilePairsList()) {
          scanList.add(new BitmapNestedLoopScan(joinPair));
        }

        currObj.setNestedLoopScanList(scanList);

        fileJoinPairs = fileJoinPairs.next;
        if (fileJoinPairs != null) {
          currObj.next = new BitmapJoinpairNestedLoopScans();
          currObj = currObj.next;
        }
      }
    }

    return condExprScansList;

  }


  public static List<BitmapCondExprValues> getNextBitmapNestedLoopJoins(
      List<BitmapJoinpairNestedLoopScans> joinPairNestedLoopScans, Boolean outerFilterVal,
      Boolean innerFilterVal)
      throws NestedLoopException, IOException, InvalidTupleSizeException {

    List<BitmapCondExprValues> condExprValuesList = new ArrayList();
    for (int idx = 0; idx < joinPairNestedLoopScans.size(); idx++) {
      BitmapCondExprValues condExprValues = new BitmapCondExprValues();
      condExprValuesList.add(condExprValues);

      BitmapJoinpairNestedLoopScans scanFile = joinPairNestedLoopScans.get(idx);

      //Current object in the list
      BitmapCondExprValues currObj = condExprValues;

      while (scanFile != null) {
        List<Boolean> valueList = new ArrayList<>();

        for (BitmapNestedLoopScan scan : scanFile.getNestedLoopScanList()) {
          Boolean val = scan.getNext(outerFilterVal, innerFilterVal);

          if (val == null) {
            //Null here indicates the end of the scan
            return null;
          }

          valueList.add(val);
        }

        currObj.setValues(valueList);

        scanFile = scanFile.next;
        if (scanFile != null) {
          currObj.setNext(new BitmapCondExprValues());
          currObj = currObj.getNext();
        }
      }
    }

    return condExprValuesList;
  }


  public static List<BitmapCondExprValues> getNext(List<BitmapCondExprScans> condExprScansList)
      throws InvalidTupleSizeException, IOException {

    List<BitmapCondExprValues> condExprValuesList = new ArrayList();
    for (int idx = 0; idx < condExprScansList.size(); idx++) {
      BitmapCondExprValues condExprValues = new BitmapCondExprValues();
      condExprValuesList.add(condExprValues);
      BitmapCondExprScans scanFile = condExprScansList.get(idx);
      //Current object in the list
      BitmapCondExprValues currObj = condExprValues;

      while (scanFile != null) {
        List<Boolean> valueList = new ArrayList<>();

        for (BitmapScan scan : scanFile.getScanList()) {
          Boolean val = scan.getNext(new RID());
          if (val == null) {
            //Null here indicates the end of the scan
            return null;
          }

          valueList.add(val);
        }

        currObj.setValues(valueList);

        scanFile = scanFile.next;
        if (scanFile != null) {
          currObj.setNext(new BitmapCondExprValues());
          currObj = currObj.getNext();
        }
      }
    }

    return condExprValuesList;

  }


  private static List<BitmapPair> getBitmapPairsForCondExpr(CondExpr condExpr,
      Columnarfile leftColumnarFile, Columnarfile rightColumnarFile) {
    int leftColNo = condExpr.operand1.symbol.offset;
    List<ColumnarHeaderRecord> leftHeaderList = leftColumnarFile.getBitMapIndicesInfo(leftColNo);

    int rightColNo = condExpr.operand2.symbol.offset;
    List<ColumnarHeaderRecord> rightHeaderList = rightColumnarFile.getBitMapIndicesInfo(rightColNo);

    return findBitMapPairs(leftHeaderList, rightHeaderList);
  }


  private static List<BitmapPair> findBitMapPairs(List<ColumnarHeaderRecord> leftBitmaps,
      List<ColumnarHeaderRecord> rightBitmaps) {
    List<BitmapPair> bitmapPairList = new ArrayList<>();
    for (ColumnarHeaderRecord leftBitmap : leftBitmaps) {
      for (ColumnarHeaderRecord rightBitmap : rightBitmaps) {
        if (rightBitmap.getValueClass().equals(leftBitmap.getValueClass())) {
          bitmapPairList.add(new BitmapPair(leftBitmap.getFileName(), rightBitmap.getFileName()));
        }
      }
    }

    return bitmapPairList;
  }


  public static List<BitmapJoinFilePairs> getBitmapJoinFilePairsForCondExpr(CondExpr[] condExprs,
      Columnarfile leftColumnarFile, Columnarfile rightColumnarFile) {
    //TODO: Construct similar structure to index
    List<BitmapJoinFilePairs> joinFilePairsList = new ArrayList();

    for (int i = 0; i < condExprs.length-1; i++) {
      CondExpr expression = condExprs[i];
      BitmapJoinFilePairs joinFilePair = new BitmapJoinFilePairs();

      joinFilePairsList.add(joinFilePair);

      //Current object in the list
      BitmapJoinFilePairs currObj = joinFilePair;
      while (expression != null) {

        List<BitmapPair> bmPairList = getBitmapPairsForCondExpr(expression, leftColumnarFile,
            rightColumnarFile);

        // Set the opened bm files to the list
        currObj.setFilePairsList(bmPairList);

        expression = expression.next;
        if (expression != null) {
          currObj.next = new BitmapJoinFilePairs();
          currObj = currObj.next;
        }
      }
    }

    return joinFilePairsList;
  }


  /**
   * Method for evaluating the condition expression for the bitmap
   */
  public static Boolean evaluateBitmapCondExpr(
      List<BitmapCondExprValues> bitmapCondExprValuesList) {
    //TODO: passing condition expression is not required - revise later
    Boolean op_res = true;

    for (int i = 0; i < bitmapCondExprValuesList.size() && op_res; i++) {
      BitmapCondExprValues condExprValues = bitmapCondExprValuesList.get(i);
      Boolean tempResult = false;
      while (condExprValues != null && !tempResult) {
        tempResult = tempResult || evaluateOR(condExprValues.getValues()); //Perform OR operations
        condExprValues = condExprValues.getNext();
      }

      op_res = op_res && tempResult;
    }

    return op_res;
  }

  /**
   * Close the bimap files opened for the bit map iterator
   */
  public static void closeBitMapFiles(List<BitmapCondExprScanFiles> scanFilesList)
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

    for (int idx = 0; idx < scanFilesList.size(); idx++) {
      BitmapCondExprScanFiles scanFile = scanFilesList.get(idx);
      while (scanFile != null) {
        for (BitMapFile file : scanFile.getScanList()) {
          file.close();
        }
        scanFile = scanFile.next;
      }
    }
  }

  /**
   * Close the bitmaps scans opened for bit map iterator
   */
  public static void closeBitMapScans(List<BitmapCondExprScans> scanFilesList)
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

    for (int idx = 0; idx < scanFilesList.size(); idx++) {
      BitmapCondExprScans scanFile = scanFilesList.get(idx);

      while (scanFile != null) {

        for (BitmapScan file : scanFile.getScanList()) {
          file.closescan();
        }

        scanFile = scanFile.next;
      }
    }
  }


  public static void closeBitmapJoinPairNestedLoopScans(
      List<BitmapJoinpairNestedLoopScans> nestedLoopScans)
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

    for (int idx = 0; idx < nestedLoopScans.size(); idx++) {
      BitmapJoinpairNestedLoopScans scanFile = nestedLoopScans.get(idx);

      while (scanFile != null) {

        for (BitmapNestedLoopScan file : scanFile.getNestedLoopScanList()) {
          file.close();
        }

        scanFile = scanFile.next;
      }
    }
  }
}
