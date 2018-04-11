package iterator;

import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import index.ColumnIndexScan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import tests.Util;
import heap.HFException;
import java.io.IOException;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import index.IndexException;
import index.UnknownIndexTypeException;
import heap.InvalidSlotNumberException;

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
		throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException, IndexException, UnknownIndexTypeException, InvalidSlotNumberException, UnknownKeyTypeException,
		IOException,
		JoinsException,
		IndexException,
		InvalidTupleSizeException,
		TupleUtilsException,
		PredEvalException,
		SortException,
		LowMemException,
		UnknowAttrType,
		UnknownKeyTypeException,
		Exception{

		switch (outerAccessType){
			case "Btree":

				Columnarfile innerCf = new Columnarfile(innerTableName);
				AttrType[] innerAttrTypes = innerCf.getType();
				IndexType innerIndexType = new IndexType(IndexType.B_Index);
				int innerIndexColumnNumber = Util.getColumnNumber(rightFilter.get(0));
				String innerRelName = innerTableName + "." + innerIndexColumnNumber;
				String innerIndName = "BTree" + innerTableName + innerIndexColumnNumber;
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
				int desiredInnerColumnNames[] = new int[innerColumnNames.size()];
				for (int i = 0; i < innerColumnNames.size(); i++) {
					desiredInnerColumnNames[i] = Util.getColumnNumber(innerColumnNames.get(i));
				}
				CondExpr[] innerExpr = Util.getValueContraint(leftFilter);
				boolean innerIndexOnly = desiredInnerColumnNames.length == 1 && !leftFilter.isEmpty() && desiredInnerColumnNames[0] == innerIndexColumnNumber;

				ColumnIndexScan innerColScan = new ColumnIndexScan(innerIndexType, innerTableName, innerRelName, innerIndName,
					innerValueConstraintAttrType, innerStrSizes, 1, desiredInnerColumnNames, innerExpr, innerIndexOnly);

				Tuple innerTuple;



				Columnarfile outerCf = new Columnarfile(outerTableName);
				AttrType[] outerAttrTypes = outerCf.getType();
				IndexType outerIndexType = new IndexType(IndexType.B_Index);
				int outerIndexColumnNumber = Util.getColumnNumber(leftFilter.get(0));
				String outerRelName = outerTableName + "." + outerIndexColumnNumber;
				String outerIndName = "BTree" + outerTableName + outerIndexColumnNumber;
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
				int desiredOuterColumnNames[] = new int[outerColumnNames.size()];
				for (int i = 0; i < outerColumnNames.size(); i++) {
					desiredOuterColumnNames[i] = Util.getColumnNumber(outerColumnNames.get(i));
				}
				CondExpr[] outerExpr = Util.getValueContraint(leftFilter);
				boolean outerIndexOnly = desiredOuterColumnNames.length == 1 && !leftFilter.isEmpty() && desiredOuterColumnNames[0] == outerIndexColumnNumber;

				ColumnIndexScan outerColScan = new ColumnIndexScan(outerIndexType, outerTableName, outerRelName, outerIndName,
					outerValueConstraintAttrType, outerStrSizes, 1, desiredOuterColumnNames, outerExpr, outerIndexOnly);

				Tuple outerTuple;

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
						if((PredEval.Eval(outerExpr, outerTuple, null, outerAttrTypes, null) && (PredEval.Eval(innerExpr, innerTuple, null, innerAttrTypes, null)) {
							CondExpr[] joinExpr = Util.getValueContraint(outFilter);
							if(PredEval.Eval(joinExpr, outerTuple, innerTuple, outerAttrTypes, innerAttrTypes)) {
//								Projection.Join(outerTuple, outerAttrTypes, innerTuple, innerAttrTypes, resultTuple, );
							}
						}
					}
				}

				break;
			case "Bitmap":
				break;
		}
	}



}
