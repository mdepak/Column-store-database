package bitmap;

import columnar.Columnarfile;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import java.io.IOException;
import java.util.List;

public class ColumnarBitmapEquiJoins {

	List<BitmapJoinFilePairs> bitmapJoinFilePairsList;
	int pairListIndex = 0;

	BitmapScan outerFilterScan;
	BitmapScan innerFilterScan;

	public ColumnarBitmapEquiJoins(
		AttrType[] in1,
		int len_in1,
		short[] t1_str_sizes,
		AttrType[] in2,
		int len_in2,
		short[] t2_str_sizes,
		int amt_of_mem,
		java.lang.String leftColumnarFileName,
		int leftJoinField,
		java.lang.String rightColumnarFileName,
		int rightJoinField,
		FldSpec[] proj_list,
		int n_out_flds, CondExpr[] joinCondExprs)
			throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException {

		Columnarfile leftColumnarFile = new Columnarfile(leftColumnarFileName);
		Columnarfile rightColumnarFile = new Columnarfile(rightColumnarFileName);

		bitmapJoinFilePairsList = BitmapUtil.getBitmapJoinFilePairsForCondExpr(joinCondExprs, leftColumnarFile, rightColumnarFile);

		//TODO: Use bitmap iterator for the filtering data based on the outer filter and the inner filter and save the results to the temp bitmap file
		outerFilterScan = null;
		innerFilterScan = null;


	}

	public Tuple getNext()
	{
		return null;
	}


	public void close()
	{
		innerFilterScan.closescan();
		outerFilterScan.closescan();
	}


}
