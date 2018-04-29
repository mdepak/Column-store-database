package iterator;

import bitmap.BitMapFile;
import btree.*;
import bufmgr.PageNotReadException;
import columnar.BitmapIterator;
import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.ColumnIndexScan;
import index.IndexException;
import index.UnknownIndexTypeException;
import tests.Util;

import java.io.IOException;

public class ColumnarIndexScan extends Iterator {

    private String _relName;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private IndexType[] _index;
    private FldSpec[] _outFlds;
    private String _colFileName;
    private Iterator[] iterators;
    public ColumnarIndexScan(String relName, String colFileName, IndexType[] index, FldSpec[] outFlds, CondExpr[] selects) throws Exception {

        try {

            Columnarfile cf = new Columnarfile(colFileName);
            _types = cf.getType();
            _s_sizes = cf.getStrSizes();

            iterators = new Iterator[10];
            for (int i = 0; i < selects.length; i++) {
                CondExpr condExpr = selects[i];
                while (condExpr != null) {
                    int colNum = condExpr.operand1.symbol.offset;
                    CondExpr[] newExpr = new CondExpr[2];
                    newExpr[0] = condExpr;
                    newExpr[1] = null;
                    AttrType[] attrType = new AttrType[1];
                    attrType[0] = cf.getType()[colNum - 1];
                    short[] strsizes = new short[1];
                    strsizes[0] = (short) 100;
                    String heapName = colFileName + "." + colNum;
                    FldSpec[] _outFlds = new FldSpec[1];
                    RelSpec rel = new RelSpec(RelSpec.outer);
                    _outFlds[0] = new FldSpec(rel, 1);
                    int[] selectedCols = new int[1];
                    switch (index[colNum - 1].indexType) {
                        case IndexType.B_Index:

                            String indName = "BTree" + colFileName + colNum;
                            AttrType[] type = new AttrType[1];
                            type[0] = new AttrType(AttrType.attrInteger);
                            ColumnIndexScan columnIndexScan = new ColumnIndexScan(new IndexType(IndexType.B_Index), relName, heapName, indName,
                                    attrType, strsizes, 1, null, newExpr, false);

                            int position = 0;
                            Heapfile heapfile = new Heapfile(indName + "temp");
                            while (position != -1) {
                                Tuple tuple = new Tuple();
                                position = columnIndexScan.get_next_pos();
                                tuple.setHdr((short) 1, type, strsizes);
                                tuple.setIntFld(1, position);
                                heapfile.insertRecord(tuple.getTupleByteArray());
                                System.out.println(position);
                            }

                            TupleOrder[] order = new TupleOrder[1];
                            order[0] = new TupleOrder(TupleOrder.Ascending);

                            FileScan fscan = new FileScan(heapName, type, strsizes, (short) 1, 1, _outFlds, null);
                            Sort sort = new Sort(_types, (short) 1, _s_sizes, fscan, 1, order[0], 10, 4);
                            iterators[i] = sort;
                            break;
                        case IndexType.BIT_MAP:
                            selectedCols[0] = 1;
                            iterators[i] = new BitmapIterator(relName, selectedCols, newExpr);
                            break;
                        case IndexType.None:
                            selectedCols[0] = 1;
                            ColumnarFileScan columnarFileScan = new ColumnarFileScan(colFileName, heapName, attrType,
                                    strsizes, (short) 1, 1, selectedCols, _outFlds, newExpr, false);
                            iterators[i] = columnarFileScan;
                            break;
                    }
                    condExpr = condExpr.next;
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple get_next()
        throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

    }


}
