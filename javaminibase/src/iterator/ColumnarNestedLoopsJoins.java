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

		//number of target columns in each of the outer and inner table
		String[] outerTargetCoulmns = outputOuterAttributesList.toArray(new String[0]);
		String[] innerTargetCoulmns = outputInnerAttributesList.toArray(new String[0]);

		//value constraints Index Type extraction
		int numOfOuterValueConstraints = getNumberOfConstraints(leftFilter);
		int numOfInnerValueConstraints = getNumberOfConstraints(rightFilter);
		IndexType[] outerValueConstraintsIndexType = getScanTypesForValueConstraints(leftFilter, numOfOuterValueConstraints, outerAccessType, outerCf);
		IndexType[] innerValueConstraintsIndexType = getScanTypesForValueConstraints(rightFilter, numOfInnerValueConstraints, innerAccessType, innerCf);

		//Scanner initializations for outer table
		AttrType[] outerAttrTypes = outerCf.getType();
		
		int outerIndexColumnNumber = Util.getColumnNumber(leftFilter.get(0)); //TODO: multiple indexes will be there. Discuss with Vinoth.
		String outerRelName = outerTableName + "." + outerIndexColumnNumber;
		AttrType[] outerValueConstraintAttrType = new AttrType[1];
		outerValueConstraintAttrType[0] = outerAttrTypes[outerIndexColumnNumber-1];
		int outerNumOfColumns = outerCf.getNumColumns();
		short[] outerStrSize = new short[outerNumOfColumns];
		int outerStrCount = 0;
		for(int i=0; i<outerNumOfColumns; i++) {
			if(outerAttrTypes[i].attrType == AttrType.attrString) {
				outerStrSize[outerStrCount] = (short)100;
				outerStrCount++;
			}
		}
		short[] outerStrSizes = Arrays.copyOfRange(outerStrSize, 0, outerStrCount);

		//TODO: Uncomment the next lines and fix compilation issue
		//outerTargetFieldValues = outerTargetFieldValues.replaceAll("\\[", "").replaceAll("\\]","");
		//String[] outerTargetCoulmns = outerTargetFieldValues.split(",");
		List<String> outerColumnNames = new ArrayList<String>();
		if(outerTargetCoulmns.length > 0 && outerTargetCoulmns != null) {
			for (String col : outerTargetCoulmns) {
				outerColumnNames.add(col);
			}
		}
		int outerTableDesiredColumnNumbers[] = new int[outerColumnNames.size()];
		for (int i = 0; i < outerColumnNames.size(); i++) {
			outerTableDesiredColumnNumbers[i] = Util.getColumnNumber(outerColumnNames.get(i));
		}
		CondExpr[] outerExpr = Util.getValueContraint(leftFilter);
		boolean outerIndexOnly = outerTableDesiredColumnNumbers.length == 1 && !leftFilter.isEmpty() && outerTableDesiredColumnNumbers[0] == outerIndexColumnNumber;
		ColumnIndexScan outerColScan;
		Tuple outerTuple;
		int numOfOuterConstraints = getNumberOfConstraints(leftFilter);
		outerValueConstraintsIndexType = new IndexType[numOfOuterConstraints];
		

		//Scanner initializations for inner table
		AttrType[] innerAttrTypes = innerCf.getType();
		int innerIndexColumnNumber = Util.getColumnNumber(rightFilter.get(0));
		String innerRelName = innerTableName + "." + innerIndexColumnNumber;
		AttrType[] innerValueConstraintAttrType = new AttrType[1];
		innerValueConstraintAttrType[0] = innerAttrTypes[innerIndexColumnNumber-1];
		int innerNumOfColumns = innerCf.getNumColumns();
		short[] innerStrSize = new short[innerNumOfColumns];
		int innerStrCount = 0;
		for(int i=0; i<innerNumOfColumns; i++) {
			if(innerAttrTypes[i].attrType == AttrType.attrString) {
				innerStrSize[innerStrCount] = (short)100;
				innerStrCount++;
			}
		}
		short[] innerStrSizes = Arrays.copyOfRange(innerStrSize, 0, innerStrCount);

		//TODO: Uncomment the next lines and fix compilation issue
		//innerTargetFieldValues = innerTargetFieldValues.replaceAll("\\[", "").replaceAll("\\]","");
		//String[] innerTargetCoulmns = innerTargetFieldValues.split(",");
		List<String> innerColumnNames = new ArrayList<String>();
		if(innerTargetCoulmns.length > 0 && innerTargetCoulmns != null) {
			for (String col : innerTargetCoulmns) {
				innerColumnNames.add(col);
			}
		}
		int innerTableDesiredColumnNumbers[] = new int[innerColumnNames.size()];
		for (int i = 0; i < innerColumnNames.size(); i++) {
			innerTableDesiredColumnNumbers[i] = Util.getColumnNumber(innerColumnNames.get(i));
		}
		CondExpr[] innerExpr = Util.getValueContraint(leftFilter);
		boolean innerIndexOnly = innerTableDesiredColumnNumbers.length == 1 && !leftFilter.isEmpty() && innerTableDesiredColumnNumbers[0] == innerIndexColumnNumber;
		ColumnIndexScan innerColScan;
		Tuple innerTuple;
		int numOfInnerConstraints = getNumberOfConstraints(rightFilter);
		innerValueConstraintsIndexType = new IndexType[numOfInnerConstraints];


		//Scan objects for both Outer and Inner tables
		if(outerAccessType.equals("BTree")) {
			outerColScan = getScanObject(outerTableName, outerIndexColumnNumber, outerRelName, outerValueConstraintAttrType, outerStrSizes, outerTableDesiredColumnNumbers, outerExpr, outerIndexOnly, 1, leftFilter);
		} else if(outerAccessType.equals("Bitmap")) {
			outerColScan = getScanObject(outerTableName, outerIndexColumnNumber, outerRelName, outerValueConstraintAttrType, outerStrSizes, outerTableDesiredColumnNumbers, outerExpr, outerIndexOnly, 2, leftFilter);
		} else {
			outerColScan = null;
		}
		if(innerAccessType.equals("BTree")) {
			innerColScan = getScanObject(innerTableName, innerIndexColumnNumber, innerRelName, innerValueConstraintAttrType, innerStrSizes, innerTableDesiredColumnNumbers, innerExpr, innerIndexOnly, 1, rightFilter);
		} else if(innerAccessType.equals("Bitmap")) {
			innerColScan = getScanObject(innerTableName, innerIndexColumnNumber, innerRelName, innerValueConstraintAttrType, innerStrSizes, innerTableDesiredColumnNumbers, innerExpr, innerIndexOnly, 2, rightFilter);
		} else {
			innerColScan = null;
		}

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

	public ColumnIndexScan getScanObject(
		String tableName,
		int indexColumnNumber,
		String relName,
		AttrType[] valueConstraintAttrType,
		short[] strSizes,
		int[] desiredColumnNumbers,
		CondExpr[] expr,
		boolean indexOnly,
		int accessMethod,
		List<String> filter)
		throws
			IndexException,
			UnknownIndexTypeException {

		IndexType indexType;
		String indName;
		String constraintValue;

		if(accessMethod == 1) {
			indexType = new IndexType(IndexType.B_Index);
			indName = "BTree" + tableName + indexColumnNumber;
		}else {
			indexType = new IndexType(IndexType.BIT_MAP);
			constraintValue = filter.get(2);
			indName = "BM_" + constraintValue + "_" + tableName + "." + indexColumnNumber;
		}
		return (new ColumnIndexScan(indexType, tableName, relName, indName, valueConstraintAttrType, strSizes, 1, desiredColumnNumbers, expr, indexOnly));
	}
}