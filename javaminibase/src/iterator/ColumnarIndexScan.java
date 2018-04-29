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
            for(int i=0; i<selects.length; i++){
                CondExpr condExpr = selects[i];
                while(condExpr != null){
                    int colNum = condExpr.operand1.symbol.offset;
                    CondExpr[] newExpr = new CondExpr[2];
                    newExpr[0] = condExpr;
                    newExpr[1] = null;
                    switch (index[colNum-1].indexType){
                        case IndexType.B_Index:
                            String heapName = colFileName + "." + colNum;
                            String indName = "BTree" + colFileName + colNum;
                            AttrType[] type = new AttrType[1];
                            type[0] = new AttrType(AttrType.attrInteger);
                            AttrType[] attrType = new AttrType[1];
                            attrType[0] = cf.getType()[colNum-1];
                            short[] strsizes = new short[1];
                            strsizes[0] = (short) 100;
                            ColumnIndexScan columnIndexScan = new ColumnIndexScan(new IndexType(IndexType.B_Index), relName, heapName, indName,
                                attrType, strsizes, 1, null, newExpr, false);

                            int position = 0;
                            Heapfile heapfile = new Heapfile(indName+"temp");
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
                            FldSpec[] _outFlds = new FldSpec[1];
                            RelSpec rel = new RelSpec(RelSpec.outer);
                            _outFlds[0] = new FldSpec(rel, 1);
                            FileScan fscan = new FileScan(heapName, type, strsizes, (short) 1, 1, _outFlds, null);
                            Sort sort = new Sort(_types, (short) 1, _s_sizes, fscan, 1, order[0], 10, 4);
                            iterators[i] = sort;
                            break;
                        case IndexType.BIT_MAP:
                            int[] outputColumnsIndexes = new int[1];
                            outputColumnsIndexes[0] = 1;
                            iterators[i] = new BitmapIterator(relName, outputColumnsIndexes, newExpr);
                            break;
                        case IndexType.None:

                            break;
                    }
                    condExpr = condExpr.next;
                }
            }


        }
        catch (Exception e){
            e.printStackTrace();
        }


//        indFile = new IndexFile[10];
//        indScan = new ColumnIndexScan[_index.length];
//        posHeap = new Heapfile[_index.length];
//        String fileName = "BTree_Position";
//        for (int i = 0; i < _index.length; i++) {
//            switch (_index[i].indexType) {
//                case IndexType.B_Index:
//                    indScan[i] = new ColumnIndexScan(new IndexType(IndexType.B_Index), relName, relName, indName[i],
//                        cf.getType()[], cf.getStrSizes(), 0, null, expr, false);
//
//                    int position = 0;
//                    posHeap[i] = new Heapfile(heapFileNames[i]);
//                    while (position != -1) {
//                        position = indScan[i].get_next_pos();
//                        tuple1.setIntFld(1, position);
//                        posHeap[i].insertRecord(tuple1.getTupleByteArray());
//                    }
//                    break;
//
//                    //TODO: Fix it after completing and writing proper method
//                    //Tuple tuple = bitmapIterator.get_next();
//                    Tuple tuple = null;
//                    break;
//            }
//        }
//        TupleOrder[] order = new TupleOrder[2];
//        order[0] = new TupleOrder(TupleOrder.Ascending);
//        order[1] = new TupleOrder(TupleOrder.Descending);
//        FileScan fscan = new FileScan(colFileName, _types, _s_sizes, (short) 1, 1, _outFlds, null);
//
//        sort = new Sort(_types, (short) 1, _s_sizes, fscan, 1, order[0], 10, 4);
    }

//    public Tuple get_next() throws Exception {
//        Tuple pos = new Tuple();
//        for (int i = 0; i < _index.length; i++) {
//             //sort the btree heapfile of positions and compare it with the bitmap position
//            if(BTpos != null) {
//                Tuple sortedPos = sort.get_next();
//                Integer sortedPostitionBT = sortedPos.getIntFld(1);
//                boolean flag = true;
//
//                Tuple firstPos = posHeap[0].getRecord(BTpos[0]);
////                for(int x = 1; x < BTRid.length ; x++)
////                {
//                if (BTpos != null) {
//                    pos = posHeap[i].getRecord(BTpos[i]);
//                    Tuple sortedBT = sort.get_next();
//                }
//
//
//            }
//                    //                }
//
//            //if flag is true construct the tuple
//
//
//
////                boolean flag = true;
////                int first = positions[0];
////                for(int j = 1; j < positions.length && flag; i++)
////                {
////                    if (positions[i] != first) flag = false;
////                }
////                if (flag)
////                {
//                    //construct the tuple
////                    while (nextentry != null) {
////                        if (index_only) {
////                            // only need to return the key
////                            AttrType[] attrType = new AttrType[1];
////                            short[] s_sizes = new short[1];
////                            if (_types[_fldNum[i] - 1].attrType == AttrType.attrInteger) {
////                                attrType[0] = new AttrType(AttrType.attrInteger);
////                                try {
////                                    Jtuple.setHdr((short) 1, attrType, s_sizes);
////                                } catch (Exception e) {
////                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
////                                }
////                                try {
////                                    Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
////                                } catch (Exception e) {
////                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
////                                }
////                            } else if (_types[_fldNum[i] - 1].attrType == AttrType.attrString) {
////                                attrType[0] = new AttrType(AttrType.attrString);
////                                // calculate string size of _fldNum
////                                int count = 0;
////                                for (int x = 0; x < _fldNum[i]; i++) {
////                                    if (_types[i].attrType == AttrType.attrString) {
////                                        count++;
////                                    }
////                                    else
////                                    return null;
////
////                                }
////                                s_sizes[0] = _s_sizes[count - 1];
////                                try {
////                                    Jtuple.setHdr((short) 1, attrType, s_sizes);
////                                } catch (Exception e) {
////                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
////                                }
////                                try {
////                                    Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
////                                } catch (Exception e) {
////                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
////                                }
////                            } else {
////                                // attrReal not supported for now
////                                throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
////                            }
////                            return Jtuple;
////                        } else {
////                            // not index_only, need to return the whole tuple
////                            rid = ((LeafData) nextentry.data).getData();
////                            int numOfOutputColumns = _outputColumnsIndexes.length;
////
////                            Columnarfile cf = new Columnarfile(_colFileName);
////                            Heapfile hf = new Heapfile(_relName);
////                            //int position = getPositionFromRID(rid, hf);
////                            AttrType[] attrType = cf.getType();
////                            AttrType[] reqAttrType = new AttrType[numOfOutputColumns];
////                            short[] s_sizes = new short[numOfOutputColumns];
////                            int j = 0;
////                            for(int y=0; y<numOfOutputColumns; y++) {
////                                reqAttrType[y] = attrType[_outputColumnsIndexes[y] - 1];
////                                if(reqAttrType[y].attrType == AttrType.attrString) {
////                                    s_sizes[j] = _s_sizes[_outputColumnsIndexes[y] - 1];
////                                    j++;
////                                }
////                            }
////                            short[] strSizes = Arrays.copyOfRange(s_sizes, 0, j);
////
////                            Tuple tuple = new Tuple();
////                            try {
////                                tuple.setHdr((short) numOfOutputColumns, reqAttrType, strSizes);
////                            } catch (InvalidTypeException e) {
////                                e.printStackTrace();
////                            }
////
////                            for(int k=0; k<numOfOutputColumns; k++){
////                                int indexNumber = _outputColumnsIndexes[k];
////                                Heapfile heapfile = cf.getColumnFiles()[indexNumber-1];
////                                Tuple tupleTemp = Util.getTupleFromPosition(positions[0], heapfile);
////                                tupleTemp.initHeaders();
////                                if(attrType[indexNumber-1].attrType == AttrType.attrString) {
////                                    tuple.setStrFld(k+1, tupleTemp.getStrFld(1));
////                                }else if(attrType[indexNumber-1].attrType == AttrType.attrInteger) {
////                                    tuple.setIntFld(k+1, tupleTemp.getIntFld(1));
////                                }else if(attrType[indexNumber-1].attrType == AttrType.attrReal) {
////                                    tuple.setFloFld(k+1, tupleTemp.getFloFld(1));
////                                }
////                            }
////                            return tuple;
////                        }
////                    }
////                    //return Jtuple;
////               // }
////
//
//        }
//        return null;
//
//    }

    @Override
    public Tuple get_next()
        throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

    }


}
