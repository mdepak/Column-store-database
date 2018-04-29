package iterator;

import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import java.io.IOException;
import java.util.*;

public class ColumnarNestedLoopsJoins {

	public List<Tuple> ColumnarNestedLoopsJoins(
		String outerTableName,
		String innerTableName,
        CondExpr[] outerConstraint,
        CondExpr[] innerConstraint,
        CondExpr[] joinConstraint,
        String outerAccessType,
		String innerAccessType,
		String targetFieldValues,
		int numOfBuffers)
		throws
            btree.ConstructPageException,
            btree.GetFileEntryException,
			Exception,
			FieldNumberOutOfBoundException,
            heap.SpaceNotAvailableException,
			HFException,
			HFBufMgrException,
			HFDiskMgrException,
			IndexException,
			InvalidSlotNumberException,
			InvalidTupleSizeException,
			InvalidTypeException,
			IOException,
			PredEvalException,
			UnknowAttrType,
			UnknownIndexTypeException,
			UnknownKeyTypeException {

		//columnar file constructors for both outer and inner tables
		Columnarfile outerCf = new Columnarfile(outerTableName);
		Columnarfile innerCf = new Columnarfile(innerTableName);

		//target columns extraction
		String outputTargetFieldValues = targetFieldValues.replaceAll("\\[", "").replaceAll("\\]","");
		String[] outputCoulmnsInOrder = outputTargetFieldValues.split(",");
		int numOfAttributesInResultTuple = outputCoulmnsInOrder.length;

		//building FldSpec for joined tuple & target column extraction for both the tables
		FldSpec[] perm_mat = new FldSpec[numOfAttributesInResultTuple];
		for(int i=0; i<numOfAttributesInResultTuple; i++) {
			String[] outputColumn = outputCoulmnsInOrder[i].split(".");
			if(outputColumn[0].equals(outerTableName)) {
				perm_mat[i].relation = new RelSpec(0);
				perm_mat[i].offset = outerCf.attrNameColNoMapping.get(outputColumn[1]);
			} else {
				perm_mat[i].relation = new RelSpec(1);
//				perm_mat[i].offset = innerCf.columnNumberOffsetMap.get(innerCf.attrNameColNoMapping.get(outputColumn[1]));
				perm_mat[i].offset = innerCf.attrNameColNoMapping.get(outputColumn[1]);
			}
		}

		//value constraints Index Type and BTree index names extraction
		IndexType[] outerTableIndexType = getIndexTypesForTable(outerAccessType, outerCf);
		IndexType[] innerTableIndexType = getIndexTypesForTable(innerAccessType, innerCf);
//		String[] outerTableIndexNames = getIndexNamesForTable(outerTableIndexType, outerCf, outerTableName);
//		String[] innerTableIndexNames = getIndexNamesForTable(innerTableIndexType, innerCf, innerTableName);

		//attribute types for both tables
		AttrType[] outerAttrTypes = outerCf.getType();
        AttrType[] innerAttrTypes = innerCf.getType();
//        short[] outerStrSizes = outerCf.getStrSizes();
//        short[] innerStrSizes = innerCf.getStrSizes();

        //output column numbers for both tables
        FldSpec[] outerFldSpec = getFldSpec(true, outerCf);
        FldSpec[] innerFldSpec = getFldSpec(false, innerCf);

		ColumnarIndexScan outerColScan = new ColumnarIndexScan(outerTableName, outerTableIndexType, outerFldSpec, outerConstraint);
		Tuple outerTuple;
        ColumnarIndexScan innerColScan = new ColumnarIndexScan(innerTableName, innerTableIndexType, innerFldSpec, innerConstraint);
        Tuple innerTuple;

		Tuple joinedTuple = new Tuple();
		List<Tuple> resultTuples = new ArrayList<Tuple>();
		while(true) {
			outerTuple = outerColScan.get_next();
			if(outerTuple == null) {
				break;
			}
			joinedTuple = null;
			while(true) {
				innerTuple = innerColScan.get_next();
				if(innerTuple == null) {
					break;
				}
				if(PredEval.Eval(joinConstraint, outerTuple, innerTuple, outerAttrTypes, innerAttrTypes)) {
					Projection.Join(outerTuple, outerAttrTypes, innerTuple, innerAttrTypes, joinedTuple, perm_mat, numOfAttributesInResultTuple);
					resultTuples.add(joinedTuple);
				}
			}
		}
		return resultTuples;
	}

	public FldSpec[] getFldSpec(boolean outer, Columnarfile cf) {
	    int numOfAttributes = cf.getNumColumns();
	    FldSpec[] fldSpec = new FldSpec[numOfAttributes];
	    for(int i=0; i<numOfAttributes; i++) {
            fldSpec[i].offset = i;
            if(outer) {
                fldSpec[i].relation = new RelSpec(0);
            }else {
                fldSpec[i].relation = new RelSpec(1);
            }
        }
	    return fldSpec;
    }

//	public int getNumberOfConstraints(List<String> filter) {
//		String[] operators = {"=", "!=", "<", ">", "<=", ">="};
//		String[] filterArray = filter.toArray(new String[0]);
//		int numOfOperators = operators.length;
//		int filterSize = filterArray.length;
//		int count = 0;
//		for(int i=0; i<numOfOperators; i++) {
//			for(int j=0; j<filterSize; j++) {
//				if(operators[i].equals(filterArray[j])) {
//					count++;
//				}
//			}
//		}
//		return count;
//	}

	public IndexType[] getIndexTypesForTable(String accessType, Columnarfile cf) {
	    int numOfAttributes = cf.getNumColumns();
		IndexType[] valueConstraintsIndexType = new IndexType[numOfAttributes];
		for(int i=0; i<numOfAttributes; i++) {
			valueConstraintsIndexType[i] = new IndexType(IndexType.None);
		}
		if(accessType.toLowerCase().equals("btree")) {
			getBitMapScanAttributes(cf, valueConstraintsIndexType);
			getBTreeScanAttributes(cf, valueConstraintsIndexType);
		}else if(accessType.toLowerCase().equals("bitmap")) {
			getBTreeScanAttributes(cf, valueConstraintsIndexType);
			getBitMapScanAttributes(cf, valueConstraintsIndexType);
		}
		return valueConstraintsIndexType;
	}

	public IndexType[] getBTreeScanAttributes(Columnarfile cf, IndexType[] valueConstraintsIndexType) {
		Set set = cf.bTreeIndexes.entrySet();
		java.util.Iterator itr = set.iterator();
		while(itr.hasNext()) {
			Map.Entry entry = (Map.Entry)itr.next();
			valueConstraintsIndexType[(int)entry.getKey()] = new IndexType(IndexType.B_Index);
		}
		return valueConstraintsIndexType;
	}

	public IndexType[] getBitMapScanAttributes(Columnarfile cf, IndexType[] valueConstraintsIndexType) {
		Set set = cf.bitmapIndexes.entrySet();
		java.util.Iterator itr = set.iterator();
		while(itr.hasNext()) {
			Map.Entry entry = (Map.Entry)itr.next();
			valueConstraintsIndexType[(int)entry.getKey()] = new IndexType(IndexType.BIT_MAP);
		}
		return valueConstraintsIndexType;
	}

//	public String[] getIndexNamesForTable(IndexType[] indexType, Columnarfile cf, String tableName) {
//        int numOfAttributes = cf.getNumColumns();
//        String[] indexNames = new String[numOfAttributes];
//	    for(int i=0; i<numOfAttributes; i++) {
//	        String type = indexType[i].toString();
//	        if(type.equals("B_Index")) {
//	            indexNames[i] = "BTree" + tableName + i;
//            }else if(type.equals("BIT_MAP")) {
//	            indexNames[i] = "BIT_MAP";
//            }else {
//	            indexNames[i] = tableName + "." + i;
//            }
//        }
//        return indexNames;
//	}
}