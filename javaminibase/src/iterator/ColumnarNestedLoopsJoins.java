package iterator;

import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnarNestedLoopsJoins {

  public ColumnarNestedLoopsJoins(
      String outerTableName,
      String innerTableName,
      CondExpr[] outerConstraint,
      CondExpr[] innerConstraint,
      CondExpr[] joinConstraint,
      String outerAccessType,
      String innerAccessType,
      String targetFieldValues,
      int numOfBuffers)
      throws Exception {

    //columnar file constructors for both outer and inner tables
    Columnarfile outerCf = new Columnarfile(outerTableName);
    Columnarfile innerCf = new Columnarfile(innerTableName);

    //target columns extraction
    String outputTargetFieldValues = targetFieldValues.replaceAll("\\[", "")
        .replaceAll("\\]", "");
    String[] outputCoulmnsInOrder = outputTargetFieldValues.split(",");
    int numOfAttributesInResultTuple = outputCoulmnsInOrder.length;

    //building FldSpec for joined tuple & target column extraction for both the tables
    FldSpec[] perm_mat = new FldSpec[numOfAttributesInResultTuple];
    for (int i = 0; i < numOfAttributesInResultTuple; i++) {
      String[] outputColumn = outputCoulmnsInOrder[i].split("\\.");
      if (outputColumn[0].equals(outerTableName)) {
        perm_mat[i] = new FldSpec(new RelSpec(0), ((int) outputColumn[1].charAt(0)) - 64);
      } else {
        perm_mat[i] = new FldSpec(new RelSpec(1), ((int) outputColumn[1].charAt(0)) - 64);
      }
    }

    //value constraints Index Type and BTree index names extraction
    IndexType[] outerTableIndexType = getIndexTypesForTable(outerAccessType, outerCf);
    IndexType[] innerTableIndexType = getIndexTypesForTable(innerAccessType, innerCf);

    AttrType[] outerAttrTypes = outerCf.getType();
    AttrType[] innerAttrTypes = innerCf.getType();

    //output column numbers for both tables
    FldSpec[] outerFldSpec = getFldSpec(true, outerCf);
    FldSpec[] innerFldSpec = getFldSpec(true, innerCf);

    ColumnarIndexScan outerColScan = new ColumnarIndexScan(outerTableName, outerTableIndexType,
        outerFldSpec, outerConstraint);


    Tuple outerTuple;

    Tuple innerTuple;

    Tuple joinedTuple = new Tuple();
    AttrType[] Jtypes = new AttrType[numOfAttributesInResultTuple];
    try {
      short[] t_size = TupleUtils
          .setup_op_tuple(joinedTuple, Jtypes, outerAttrTypes, outerAttrTypes.length,
              innerAttrTypes, innerAttrTypes.length, outerCf.getStrSizes(), innerCf.getStrSizes(),
              perm_mat, numOfAttributesInResultTuple);
    } catch (TupleUtilsException e) {
      throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
    }
    int outerPos = 0;
    int innerPos = 0;

    List<Tuple> resultTuples = new ArrayList<Tuple>();
    while (true) {
      outerTuple = outerColScan.get_next();
      outerPos++;
      if (outerTuple == null) {
        outerColScan.close();
        break;
      }

      ColumnarIndexScan innerColScan = new ColumnarIndexScan(innerTableName, innerTableIndexType,
          innerFldSpec, innerConstraint);

      while (true) {
        innerTuple = innerColScan.get_next();
        if (innerTuple == null) {
          innerColScan.close();
          break;
        }
        if (PredEval.Eval(joinConstraint, outerTuple, innerTuple, outerAttrTypes, innerAttrTypes)) {
          Projection
              .Join(outerTuple, outerAttrTypes, innerTuple, innerAttrTypes, joinedTuple, perm_mat,
                  numOfAttributesInResultTuple);
          resultTuples.add(new Tuple(joinedTuple));

          System.out.println("Joined tuples  outer: " + outerPos + "   inner pos :" + innerPos);
        }
        innerPos++;
      }
    }
    printJoinedTuples(resultTuples, outerAttrTypes, innerAttrTypes, perm_mat);
  }

  public void printJoinedTuples(List<Tuple> joinedTuples, AttrType[] outerAttributes,
      AttrType[] innerAttributes, FldSpec[] fldSpecs)
      throws IOException, FieldNumberOutOfBoundException {
    java.util.Iterator itr = joinedTuples.iterator();
    while (itr.hasNext()) {
      Tuple tuple = (Tuple) itr.next();
      for (int idx = 0; idx < fldSpecs.length; idx++) {
        AttrType fldAttr = null;

        int tableNo = fldSpecs[idx].relation.key;
        if (tableNo == 0) {
          fldAttr = outerAttributes[fldSpecs[idx].offset - 1];
        } else {
          fldAttr = innerAttributes[fldSpecs[idx].offset - 1];
        }

        switch (fldAttr.attrType) {
          case AttrType.attrInteger:
            System.out.print(tuple.getIntFld(idx + 1) + "\t");
            break;
          case AttrType.attrString:
            System.out.print(tuple.getStrFld(idx + 1) + "\t");
            break;
        }
      }
      System.out.println();
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

  public static IndexType[] getIndexTypesForTable(String accessType, Columnarfile cf) {
    int numOfAttributes = cf.getNumColumns();
    IndexType[] valueConstraintsIndexType = new IndexType[numOfAttributes];
    for (int i = 0; i < numOfAttributes; i++) {
      valueConstraintsIndexType[i] = new IndexType(IndexType.None);
    }
    if (accessType.toLowerCase().equals("btree")) {
      getBitMapScanAttributes(cf, valueConstraintsIndexType);
      getBTreeScanAttributes(cf, valueConstraintsIndexType);
    } else if (accessType.toLowerCase().equals("bitmap")) {
      getBTreeScanAttributes(cf, valueConstraintsIndexType);
      getBitMapScanAttributes(cf, valueConstraintsIndexType);
    }
    return valueConstraintsIndexType;
  }

  public static IndexType[] getBTreeScanAttributes(Columnarfile cf,
      IndexType[] valueConstraintsIndexType) {
    Set set = cf.bTreeIndexes.entrySet();
    java.util.Iterator itr = set.iterator();
    while (itr.hasNext()) {
      Map.Entry entry = (Map.Entry) itr.next();
      valueConstraintsIndexType[(int) entry.getKey() - 1] = new IndexType(IndexType.B_Index);
    }
    return valueConstraintsIndexType;
  }

  public static IndexType[] getBitMapScanAttributes(Columnarfile cf,
      IndexType[] valueConstraintsIndexType) {
    Set set = cf.bitmapIndexes.entrySet();
    java.util.Iterator itr = set.iterator();
    while (itr.hasNext()) {
      Map.Entry entry = (Map.Entry) itr.next();
      valueConstraintsIndexType[(int) entry.getKey() - 1] = new IndexType(IndexType.BIT_MAP);
    }
    return valueConstraintsIndexType;
  }
}