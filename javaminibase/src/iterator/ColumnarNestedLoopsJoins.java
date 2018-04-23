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

	public List<Tuple> ColumnarNestedLoopsJoins(
		String outerTableName,
		String innerTableName,
		List<String> leftFilter,
		List<String> rightFilter,
		List<String> outputFilter,
		String innerAccessType,
		String outerAccessType,
		String targetFieldValues,
		int numOfBuffers)
		throws
			FieldNumberOutOfBoundException,
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

		//columnar file object for outer and inner tables
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

		//number of target columns in each of the outer and inner table
		String[] outerTargetColumns = outputOuterAttributesList.toArray(new String[0]);
		String[] innerTargetColumns = outputInnerAttributesList.toArray(new String[0]);

		//value constraints extraction
		int numOfOuterValueConstraints = getNumberOfConstraints(leftFilter);
		int numOfInnerValueConstraints = getNumberOfConstraints(rightFilter);
		IndexType[] outerValueConstraintsIndexType = getScanTypesForValueConstraints(leftFilter, numOfOuterValueConstraints, outerAccessType, outerCf);
		IndexType[] innerValueConstraintsIndexType = getScanTypesForValueConstraints(rightFilter, numOfInnerValueConstraints, innerAccessType, innerCf);
		CondExpr[] outerExpr = Util.getValueContraint(leftFilter);
		CondExpr[] innerExpr = Util.getValueContraint(rightFilter);
		int[] outerValueConstraintsColumnNumbers = getValueConstraintsColumnNumber(leftFilter, outerCf);
		int[] innerValueConstraintsColumnNumbers = getValueConstraintsColumnNumber(rightFilter, innerCf);

		//Scanner initializations for outer table
		AttrType[] outerAttrTypes = outerCf.getType();
		short[] outerStrSizes = outerCf.getStrSizes();
//		int outerIndexColumnNumber = Util.getColumnNumber(leftFilter.get(0)); //TODO: multiple indexes will be there. Discuss with Vinoth.
//		String outerRelName = outerTableName + "." + outerIndexColumnNumber;
//		AttrType[] outerValueConstraintAttrType = new AttrType[1];
//		outerValueConstraintAttrType[0] = outerAttrTypes[outerIndexColumnNumber-1];
//		int outerNumOfColumns = outerCf.getNumColumns();
//		short[] outerStrSize = new short[outerNumOfColumns];
//		int outerStrCount = 0;
//		for(int i=0; i<outerNumOfColumns; i++) {
//			if(outerAttrTypes[i].attrType == AttrType.attrString) {
//				outerStrSize[outerStrCount] = (short)100;
//				outerStrCount++;
//			}
//		}
//		short[] outerStrSizes = Arrays.copyOfRange(outerStrSize, 0, outerStrCount);
//		outerTargetFieldValues = outerTargetFieldValues.replaceAll("\\[", "").replaceAll("\\]","");
//		String[] outerTargetColumns = outerTargetFieldValues.split(",");
//		List<String> outerColumnNames = new ArrayList<String>();
//		if(outerTargetColumns.length > 0 && outerTargetColumns != null) {
//			for (String col : outerTargetColumns) {
//				outerColumnNames.add(col);
//			}
//		}
//		int outerTableDesiredColumnNumbers[] = new int[outerColumnNames.size()];
//		for (int i = 0; i < outerColumnNames.size(); i++) {
//			outerTableDesiredColumnNumbers[i] = Util.getColumnNumber(outerColumnNames.get(i));
//		}
//		boolean outerIndexOnly = outerTableDesiredColumnNumbers.length == 1 && !leftFilter.isEmpty() && outerTableDesiredColumnNumbers[0] == outerIndexColumnNumber;
		ColumnarIndexScan outerColScan = getScanObject(outerAccessType, outerTableName, outerValueConstraintsColumnNumbers, outerValueConstraintsIndexType, outerAttrTypes, outerStrSizes, outerExpr);
		Tuple outerTuple;
		

		//Scanner initializations for inner table
		AttrType[] innerAttrTypes = innerCf.getType();
		short[] innerStrSizes = innerCf.getStrSizes();
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
//		innerTargetFieldValues = innerTargetFieldValues.replaceAll("\\[", "").replaceAll("\\]","");
//		String[] innerTargetColumns = innerTargetFieldValues.split(",");
//		List<String> innerColumnNames = new ArrayList<String>();
//		if(innerTargetColumns.length > 0 && innerTargetColumns != null) {
//			for (String col : innerTargetColumns) {
//				innerColumnNames.add(col);
//			}
//		}
//		int innerTableDesiredColumnNumbers[] = new int[innerColumnNames.size()];
//		for (int i = 0; i < innerColumnNames.size(); i++) {
//			innerTableDesiredColumnNumbers[i] = Util.getColumnNumber(innerColumnNames.get(i));
//		}
//		boolean innerIndexOnly = innerTableDesiredColumnNumbers.length == 1 && !leftFilter.isEmpty() && innerTableDesiredColumnNumbers[0] == innerIndexColumnNumber;
		ColumnarIndexScan innerColScan = getScanObject(innerAccessType, innerTableName, innerValueConstraintsColumnNumbers, innerValueConstraintsIndexType, innerAttrTypes, innerStrSizes, innerExpr);
		Tuple innerTuple;

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
				CondExpr[] joinExpr = Util.getValueContraint(outputFilter);
				if(PredEval.Eval(joinExpr, outerTuple, innerTuple, outerAttrTypes, innerAttrTypes)) {
					Projection.Join(outerTuple, outerAttrTypes, innerTuple, innerAttrTypes, joinedTuple, perm_mat, numOfAttributesInResultTuple);
					resultTuples.add(joinedTuple);
				}
			}
		}
		return resultTuples;
	}

	public int[] getValueConstraintsColumnNumber(List<String>filter, Columnarfile cf) {
		int[] columnNumbers = new int[filter.size()];
		String[] operators = {"=", "!=", "<", ">", "<=", ">="};
		String[] filterArray = filter.toArray(new String[0]);
		int numOfOperators = operators.length;
		int filterSize = filterArray.length;
		int k = 0;
		for(int i=0; i<filterSize; i++) {
			for(int j=0; j<numOfOperators; j++) {
				if(filterArray[i].equals(operators[j])) {
					columnNumbers[k] = cf.attrNameColNoMapping.get(filterArray[i-1]);
					k++;
				}
			}
		}
		return columnNumbers;
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

	public IndexType[] getScanTypesForValueConstraints(List<String> filter, int numOfConstraints, String accessType, Columnarfile cf) {
		IndexType[] valueConstraintsIndexType = new IndexType[numOfConstraints];
		for(int i=0; i<numOfConstraints; i++) {
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

	public ColumnarIndexScan getScanObject(
		String accessType,
		String tableName,
		int[] valueConstraintsColumnNumber,
		IndexType[] valueConstraintsIndexType,
		AttrType[] attrType,
		short[] strSizes,
		CondExpr[] expr)
		throws
			IndexException,
			IOException,
			UnknownIndexTypeException {

		IndexType indexType;
		String indName;
		String constraintValue;

		switch(accessType) {
			case "BTree" :
				indexType = new IndexType(IndexType.B_Index);
				indName = "BTree" + tableName + indexColumnNumber;
				break;

			case "Bitmap" :
				indexType = new IndexType(IndexType.BIT_MAP);
				constraintValue = filter.get(2);
				indName = "BM_" + constraintValue + "_" + tableName + "." + indexColumnNumber;
				break;

			default :
				indexType = null;
				indName = null;
		}
		return (new ColumnarIndexScan(tableName, valueConstraintsColumnNumber, valueConstraintsIndexType, attrType, strSizes,expr));
	}
}