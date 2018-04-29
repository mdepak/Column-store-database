package iterator;

import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import heap.*;
import index.ColumnIndexScan;
import index.IndexException;
import index.UnknownIndexTypeException;
import java.io.IOException;
import java.util.*;
import tests.Util;

public class ColumnarNestedLoopsJoins {
	List<Tuple> joinedTuples;

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
		throws
            btree.ConstructPageException,
            btree.GetFileEntryException,
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
		int outputOuterAttributesCount = 0;
		List<String> outputOuterAttributesList = new ArrayList<String>();
		int outputInnerAttributesCount = 0;
		List<String> outputInnerAttributesList = new ArrayList<String>();

		//building FldSpec for joined tuple & target column extraction for both the tables
		FldSpec[] perm_mat = new FldSpec[numOfAttributesInResultTuple];
		for(int i=0; i<numOfAttributesInResultTuple; i++) {
			String[] outputColumn = outputCoulmnsInOrder[i].split(".");
			if(outputColumn[0].equals(outerTableName)) {
				outputOuterAttributesCount++;
				outputOuterAttributesList.add(outputColumn[1]);
				perm_mat[i].relation = new RelSpec(0);
				perm_mat[i].offset = outerCf.columnNumberOffsetMap.get(outerCf.attrNameColNoMapping.get(outputColumn[1]));
			} else {
				outputInnerAttributesCount++;
				outputInnerAttributesList.add(outputColumn[1]);
				perm_mat[i].relation = new RelSpec(1);
				perm_mat[i].offset = innerCf.columnNumberOffsetMap.get(innerCf.attrNameColNoMapping.get(outputColumn[1]));
			}
		}

//		//number of target columns in each of the outer and inner table
//		String[] outerTargetCoulmns = outputOuterAttributesList.toArray(new String[0]);
//		String[] innerTargetCoulmns = outputInnerAttributesList.toArray(new String[0]);

		//value constraints Index Type and BTree index names extraction
		IndexType[] outerTableIndexType = getIndexTypesForTable(outerAccessType, outerCf);
		IndexType[] innerTableIndexType = getIndexTypesForTable(innerAccessType, innerCf);
		String[] outerTableIndexNames = getIndexNamesForTable(outerTableIndexType, outerCf);
		String[] innerTableIndexNames = getIndexNamesForTable(innerTableIndexType, innerCf);

		//attribute types for both tables
		AttrType[] outerAttrTypes = outerCf.getType();
        AttrType[] innerAttrTypes = innerCf.getType();
        short[] outerStrSizes = outerCf.getStrSizes();
        short[] innerStrSizes = innerCf.getStrSizes();

        //output column numbers for both tables
        FldSpec[] outerFldSpec = getFldSpec(true, outerCf);
        FldSpec[] innerFldSpec = getFldSpec(false, innerCf);

		ColumnarIndexScan outerColScan = new ColumnarIndexScan(outerTableName, null, null, outerTableIndexType, outerTableIndexNames, outerAttrTypes, outerStrSizes, 0, 0, outerFldSpec, outerConstraint, false);
		Tuple outerTuple;
        ColumnarIndexScan innerColScan = new ColumnarIndexScan(innerTableName, null, null, innerTableIndexType, innerTableIndexNames, innerAttrTypes, innerStrSizes, 0, 0, innerFldSpec, innerConstraint, false);
        Tuple innerTuple;


//		//Scanner initializations for inner table
//		int innerIndexColumnNumber = Util.getColumnNumber(rightFilter.get(0));
//		String innerRelName = innerTableName + "." + innerIndexColumnNumber;
//		AttrType[] innerValueConstraintAttrType = new AttrType[1];
//		innerValueConstraintAttrType[0] = innerAttrTypes[innerIndexColumnNumber-1];
//		int innerNumOfColumns = innerCf.getNumColumns();
//		short[] innerStrSize = new short[innerNumOfColumns];
//		int innerStrCount = 0;
//		for(int i=0; i<innerNumOfColumns; i++) {
//			if(innerAttrTypes[i].attrType == AttrType.attrString) {
//				innerStrSize[innerStrCount] = (short)100;
//				innerStrCount++;
//			}
//		}
//		short[] innerStrSizes = Arrays.copyOfRange(innerStrSize, 0, innerStrCount);
//		//innerTargetFieldValues = innerTargetFieldValues.replaceAll("\\[", "").replaceAll("\\]","");
//		//String[] innerTargetCoulmns = innerTargetFieldValues.split(",");
//		List<String> innerColumnNames = new ArrayList<String>();
//		if(innerTargetCoulmns.length > 0 && innerTargetCoulmns != null) {
//			for (String col : innerTargetCoulmns) {
//				innerColumnNames.add(col);
//			}
//		}
//		int innerTableDesiredColumnNumbers[] = new int[innerColumnNames.size()];
//		for (int i = 0; i < innerColumnNames.size(); i++) {
//			innerTableDesiredColumnNumbers[i] = Util.getColumnNumber(innerColumnNames.get(i));
//		}
//		CondExpr[] innerExpr = Util.getValueContraint(leftFilter);
//		boolean innerIndexOnly = innerTableDesiredColumnNumbers.length == 1 && !leftFilter.isEmpty() && innerTableDesiredColumnNumbers[0] == innerIndexColumnNumber;
//		int numOfInnerConstraints = getNumberOfConstraints(rightFilter);
//		innerTableIndexType = new IndexType[numOfInnerConstraints];


//		//Scan objects for both Outer and Inner tables
//		if(outerAccessType.equals("BTree")) {
//			outerColScan = getScanObject(outerTableName, outerIndexColumnNumber, outerRelName, outerValueConstraintAttrType, outerStrSizes, outerTableDesiredColumnNumbers, outerExpr, outerIndexOnly, 1, leftFilter);
//		} else if(outerAccessType.equals("Bitmap")) {
//			outerColScan = getScanObject(outerTableName, outerIndexColumnNumber, outerRelName, outerValueConstraintAttrType, outerStrSizes, outerTableDesiredColumnNumbers, outerExpr, outerIndexOnly, 2, leftFilter);
//		} else {
//			outerColScan = null;
//		}
//		if(innerAccessType.equals("BTree")) {
//			innerColScan = getScanObject(innerTableName, innerIndexColumnNumber, innerRelName, innerValueConstraintAttrType, innerStrSizes, innerTableDesiredColumnNumbers, innerExpr, innerIndexOnly, 1, rightFilter);
//		} else if(innerAccessType.equals("Bitmap")) {
//			innerColScan = getScanObject(innerTableName, innerIndexColumnNumber, innerRelName, innerValueConstraintAttrType, innerStrSizes, innerTableDesiredColumnNumbers, innerExpr, innerIndexOnly, 2, rightFilter);
//		} else {
//			innerColScan = null;
//		}

		//One joined result Tuple
		Tuple joinedTuple = new Tuple();
		//All joined tuples
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
		joinedTuples = resultTuples;
	}

	public List<Tuple> getJoinedTuples() {
		return joinedTuples;
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

	public int getNumberOfConstraints(List<String> filter) {
		String[] operators = {"=", "!=", "<", ">", "<=", ">="};
		String[] filterArray = filter.toArray(new String[0]);
		int numOfOperators = operators.length;
		int filterSize = filterArray.length;
		int count = 0;
		for(int i=0; i<numOfOperators; i++) {
			for(int j=0; j<filterSize; j++) {
				if(operators[i].equals(filterArray[j])) {
					count++;
				}
			}
		}
		return count;
	}

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

	public String[] getIndexNamesForTable(IndexType[] indexType, Columnarfile cf) {
        int numOfAttributes = cf.getNumColumns();
        String[] indexNames = new String[numOfAttributes];
	    for(int i=0; i<numOfAttributes; i++) {
	        String type = indexType[i].toString();
	        if(type.equals("B_Index")) {
	            indexNames[i] = "BTree" + cf + i;
            }else if(type.equals("BIT_MAP")) {
	            indexNames[i] = "BIT_MAP";
            }else {
	            indexNames[i] = cf + "." + i;
            }
        }
        return indexNames;
	}
}