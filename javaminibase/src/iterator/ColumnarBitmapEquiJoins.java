package iterator;

import columnar.ColumnarHeaderRecord;
import columnar.Columnarfile;
import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnarBitmapEquiJoins {

	List<BitmapPair> bitmapPairList;
	int pairListIndex = 0;

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
		int n_out_flds)
			throws InvalidTupleSizeException, HFException, IOException, FieldNumberOutOfBoundException, HFBufMgrException, HFDiskMgrException {

		Columnarfile leftColumnarFile = new Columnarfile(leftColumnarFileName);
		Columnarfile rightColumnarFile = new Columnarfile(rightColumnarFileName);

		bitmapPairList = findBitMapPairs(leftColumnarFile, rightColumnarFile, leftJoinField, rightJoinField);


	}


	public Tuple getNext()
	{
		return null;
	}


	public void close()
	{

	}

	private class BitmapPair
	{
		private String leftBitmapFile;
		private String rightBitmapFile;

		public String getLeftBitmapFile() {
			return leftBitmapFile;
		}

		public void setLeftBitmapFile(String leftBitmapFile) {
			this.leftBitmapFile = leftBitmapFile;
		}

		public String getRightBitmapFile() {
			return rightBitmapFile;
		}

		public void setRightBitmapFile(String rightBitmapFile) {
			this.rightBitmapFile = rightBitmapFile;
		}

		public BitmapPair(String leftBitmapFile, String rightBitmapFile) {
			this.leftBitmapFile = leftBitmapFile;
			this.rightBitmapFile = rightBitmapFile;
		}

	}


	private List<BitmapPair> findBitMapPairs(Columnarfile leftColumnarFile, Columnarfile rightColumanrFile, int leftJoinField, int rightJoinField)
	{
		List<ColumnarHeaderRecord> leftBitmaps = leftColumnarFile.getBitMapIndicesInfo(leftJoinField);
		List<ColumnarHeaderRecord> rightBitmaps = rightColumanrFile.getBitMapIndicesInfo(rightJoinField);


		List<BitmapPair> bitmapPairList = new ArrayList<>();
		for(ColumnarHeaderRecord leftBitmap : leftBitmaps)
		{
			for(ColumnarHeaderRecord rightBitmap : rightBitmaps)
			{
				if(rightBitmap.getValueClass().equals(leftBitmap.getValueClass()))
				{
					bitmapPairList.add(new BitmapPair(leftBitmap.getFileName(), rightBitmap.getFileName()));
				}
			}
		}

		return bitmapPairList;
	}

}
