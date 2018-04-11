package iterator;

import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import heap.*;
import index.ColumnIndexScan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.w3c.dom.Attr;
import tests.Util;

import java.io.IOException;

import index.IndexException;
import index.UnknownIndexTypeException;

public class ColumnarNestedLoopsJoins {

	public ColumnarNestedLoopsJoins(
		AttrType[] in1,
		int len_in1,
		short[] t1_str_sizes,
		AttrType[] in2,
		int len_in2,
		short[] t2_str_sizes,
		int amt_of_mem,
		Iterator am1,
		java.lang.String outerTableName,
		java.lang.String innerTableName,
		List<String> leftFilter,
		List<String> rightFilter,
		List<String> outFilter,
		String innerAccessType,
		String outerAccessType,
		String outerTargetColumns,
		String innerTargetColumns,
		FldSpec[] proj_list,
		int n_out_flds)
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

		//Scanner initializations for outer table
		Columnarfile outerCf = new Columnarfile(outerTableName);
		AttrType[] outerAttrTypes = outerCf.getType();
//		IndexType outerIndexType = new IndexType(IndexType.B_Index);
		IndexType outerIndexType;
		int outerIndexColumnNumber = Util.getColumnNumber(leftFilter.get(0));
		String outerRelName = outerTableName + "." + outerIndexColumnNumber;
//		String outerIndName = "BTree" + outerTableName + outerIndexColumnNumber;
		String outerIndName;
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
		outerTargetColumns = outerTargetColumns.replaceAll("\\[", "").replaceAll("\\]","");
		String[] outerColArray = outerTargetColumns.split(",");
		List<String> outerColumnNames = new ArrayList<String>();
		if(outerColArray.length > 0 && outerColArray != null) {
			for (String col : outerColArray) {
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


		//Scanner initializations for inner table
		Columnarfile innerCf = new Columnarfile(innerTableName);
		AttrType[] innerAttrTypes = innerCf.getType();
//		IndexType innerIndexType = new IndexType(IndexType.B_Index);
		IndexType innerIndexType;
		int innerIndexColumnNumber = Util.getColumnNumber(rightFilter.get(0));
		String innerRelName = innerTableName + "." + innerIndexColumnNumber;
//		String innerIndName = "BTree" + innerTableName + innerIndexColumnNumber;
		String innerIndName;
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
		innerTargetColumns = innerTargetColumns.replaceAll("\\[", "").replaceAll("\\]","");
		String[] innerColArray = innerTargetColumns.split(",");
		List<String> innerColumnNames = new ArrayList<String>();
		if(innerColArray.length > 0 && innerColArray != null) {
			for (String col : innerColArray) {
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

		//Scan objects for both Outer and Inner tables
		int outerAccessMethod = 2;
		int innerAccessMethod = 2;
		switch (outerAccessType) {
			case "Btree":
				outerAccessMethod = 0;
				break;

			case "Bitmap":
				outerAccessMethod = 1;
				break;
		}
		outerColScan = getScanObject(outerTableName, outerIndexColumnNumber, outerRelName, outerValueConstraintAttrType, outerStrSizes, outerTableDesiredColumnNumbers, outerExpr, outerIndexOnly, outerAccessMethod, leftFilter);
		switch (innerAccessType) {
			case "Btree":
				innerAccessMethod = 0;
				break;

			case "Bitmap":
				innerAccessMethod = 1;
				break;
		}
		innerColScan = getScanObject(innerTableName, innerIndexColumnNumber, innerRelName, innerValueConstraintAttrType, innerStrSizes, innerTableDesiredColumnNumbers, innerExpr, innerIndexOnly, innerAccessMethod, rightFilter);

		//One joined result Tuple
		Tuple resultTuple = new Tuple();

		while(true) {
			outerTuple = outerColScan.get_next();
			if(outerTuple == null) {
				break;
			}
			resultTuple = null;
			while(true) {
				innerTuple = innerColScan.get_next();
				if(innerTuple == null) {
					break;
				}
				if(PredEval.Eval(outerExpr, outerTuple, null, outerAttrTypes, null) && PredEval.Eval(innerExpr, innerTuple, null, innerAttrTypes, null)) {
					CondExpr[] joinExpr = Util.getValueContraint(outFilter);
					if(PredEval.Eval(joinExpr, outerTuple, innerTuple, outerAttrTypes, innerAttrTypes)) {
//								Projection.Join(outerTuple, outerAttrTypes, innerTuple, innerAttrTypes, resultTuple, );
					}
				}
			}
		}
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

		if(accessMethod == 0) {
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