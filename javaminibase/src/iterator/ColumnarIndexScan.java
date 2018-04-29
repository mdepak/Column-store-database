package iterator;

import bitmap.BitMapFile;
import btree.*;
import columnar.BitmapIterator;
import columnar.Columnarfile;
import columnar.Util;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;
import index.IndexUtils;
import index.UnknownIndexTypeException;

import java.io.IOException;
import java.util.Arrays;

import static columnar.Util.getPositionFromRID;
import static columnar.Util.getRIDFromPosition;

public class ColumnarIndexScan extends Iterator {

    public FldSpec[] perm_mat;
    //private String _colFileName;
    private String _relName;
    private String[] _indName;
    private IndexFile[] indFile;
    private BitMapFile[] bitMapFile;
    private IndexFileScan[] indScan;
    private AttrType[] _types;
    private short[] _s_sizes;
    private CondExpr[] _selects;
    private int[] _outputColumnsIndexes;
    private int _noInFlds;
    private int _noOutFlds;
    private Heapfile f;
    private Tuple tuple1;
    private Tuple Jtuple;
    private int t1_size;
    private int[] _fldNum;
    private boolean index_only;
    private Scan bitMapScan;
    private IndexType[] _index;
    private FldSpec[] _outFlds;
    private String _colFileName;
    private Integer bitmapPos;
    private RID[] ridAndOp;
    private Heapfile[] position_idx;
    private String[] heapFileNames;
    private Sort sort;
    private RID rid = null;
    private RID[] BTpos = new RID[_index.length];
    private KeyDataEntry nextentry = null;
    private int[] positions = new int[_index.length];
    private BitmapIterator[] bmiter;
    ColumnarIndexScan(String relName,
                      final String colFileName,
                      int[] fldNum,
                      IndexType[] index,
                      String[] indName,
                      AttrType[] types,
                      short[] str_sizes,
                      int noInFlds,
                      int noOutFlds,
                      FldSpec[] outFlds,
                      CondExpr[] selects,
                      boolean indexOnly) throws IndexException, UnknownIndexTypeException, IOException, SortException, FileScanException, TupleUtilsException, InvalidRelation, HFDiskMgrException, HFBufMgrException, HFException, ConstructPageException, GetFileEntryException, FieldNumberOutOfBoundException, InvalidTupleSizeException, InvalidSlotNumberException, InvalidTypeException, SpaceNotAvailableException {

        _relName = relName;
        _fldNum = fldNum;
        _index = index;
        _indName = indName;
        _types = types;
        _s_sizes = str_sizes;
        _outFlds = outFlds;
        _selects = selects;
        index_only = indexOnly;
        _colFileName = colFileName;
        short[] ts_sizes;
        Jtuple = new Tuple();
        AttrType[] Jtypes = new AttrType[noOutFlds];

       // position_idx = new Heapfile[_index.length];
        heapFileNames = new String[_index.length];
        String fileName = "BTree_Position";
        for (int i = 0; i < _index.length; i++) {

            switch (_index[i].indexType) {
                case IndexType.B_Index:
                    if (indFile != null) {
                        try {
                            nextentry = indScan[i].get_next();
                        } catch (Exception e) {
                            throw new IndexException(e, "IndexScan.java: BTree error");
                        }
                        try {
                            indFile[i] = new BTreeFile(indName[i]);
                        } catch (Exception e) {
                            throw new IndexException(e,
                                    "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                        }
                        try {
                            indScan[i] = (BTFileScan) IndexUtils.BTree_scan(selects, indFile[i]);
                        } catch (Exception e) {
                            throw new IndexException(e,
                                    "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                        }
                        //position_idx[i] = new Heapfile(heapFileNames[i]);

                        // linear hashing is not yet implemented

                        position_idx[i] = new Heapfile(heapFileNames[i]);
                        while (nextentry != null) {
                            rid = ((LeafData) nextentry.data).getData();
                            int position = rid.position;
                            tuple1.setIntFld(1, position);
                            BTpos[i] = position_idx[i].insertRecord(tuple1.getTupleByteArray());
                        }
                    }
                    break;

                case IndexType.BIT_MAP:
                    Columnarfile file = new Columnarfile(_colFileName);
                    int bitMapIndexCol = _selects[i].columnNo;
                    bmiter[i] = new BitmapIterator(_relName, _outputColumnsIndexes, _selects);
                    // bitmapPos = bmiter[i].get_next();

                    //CondExpr[] condExprs = tests.Util.getValueContraint(valueConstraint);

//                        BitmapIterator bitmapIterator = new BitmapIterator(_colFileName, bitMapIndexCol,
//                                selectCols, _selects, false);

                    //TODO: Fix it after completing and writing proper method
                    //Tuple tuple = bitmapIterator.get_next();
                    Tuple tuple = null;
                    break;
            }
        }
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
        FileScan fscan = new FileScan(colFileName, _types, _s_sizes, (short) 1, 1, _outFlds, null);

        sort = new Sort(_types, (short) 1, _s_sizes, fscan, 1, order[0], 10, 4);
    }

    public Tuple get_next() throws Exception {
        Tuple pos = new Tuple();
        for (int i = 0; i < _index.length; i++) {
             //sort the btree heapfile of positions and compare it with the bitmap position
            if(BTpos != null) {
                Tuple sortedPos = sort.get_next();
                Integer sortedPostitionBT = sortedPos.getIntFld(1);
                boolean flag = true;

                Tuple firstPos = position_idx[0].getRecord(BTpos[0]);
//                for(int x = 1; x < BTRid.length ; x++)
//                {
                if (BTpos != null) {
                    pos = position_idx[i].getRecord(BTpos[i]);
                    Tuple sortedBT = sort.get_next();
                }


            }
                    //                }

            //if flag is true construct the tuple



//                boolean flag = true;
//                int first = positions[0];
//                for(int j = 1; j < positions.length && flag; i++)
//                {
//                    if (positions[i] != first) flag = false;
//                }
//                if (flag)
//                {
                    //construct the tuple
//                    while (nextentry != null) {
//                        if (index_only) {
//                            // only need to return the key
//                            AttrType[] attrType = new AttrType[1];
//                            short[] s_sizes = new short[1];
//                            if (_types[_fldNum[i] - 1].attrType == AttrType.attrInteger) {
//                                attrType[0] = new AttrType(AttrType.attrInteger);
//                                try {
//                                    Jtuple.setHdr((short) 1, attrType, s_sizes);
//                                } catch (Exception e) {
//                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
//                                }
//                                try {
//                                    Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
//                                } catch (Exception e) {
//                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
//                                }
//                            } else if (_types[_fldNum[i] - 1].attrType == AttrType.attrString) {
//                                attrType[0] = new AttrType(AttrType.attrString);
//                                // calculate string size of _fldNum
//                                int count = 0;
//                                for (int x = 0; x < _fldNum[i]; i++) {
//                                    if (_types[i].attrType == AttrType.attrString) {
//                                        count++;
//                                    }
//                                    else
//                                    return null;
//
//                                }
//                                s_sizes[0] = _s_sizes[count - 1];
//                                try {
//                                    Jtuple.setHdr((short) 1, attrType, s_sizes);
//                                } catch (Exception e) {
//                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
//                                }
//                                try {
//                                    Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
//                                } catch (Exception e) {
//                                    throw new IndexException(e, "IndexScan.java: Heapfile error");
//                                }
//                            } else {
//                                // attrReal not supported for now
//                                throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
//                            }
//                            return Jtuple;
//                        } else {
//                            // not index_only, need to return the whole tuple
//                            rid = ((LeafData) nextentry.data).getData();
//                            int numOfOutputColumns = _outputColumnsIndexes.length;
//
//                            Columnarfile cf = new Columnarfile(_colFileName);
//                            Heapfile hf = new Heapfile(_relName);
//                            //int position = getPositionFromRID(rid, hf);
//                            AttrType[] attrType = cf.getType();
//                            AttrType[] reqAttrType = new AttrType[numOfOutputColumns];
//                            short[] s_sizes = new short[numOfOutputColumns];
//                            int j = 0;
//                            for(int y=0; y<numOfOutputColumns; y++) {
//                                reqAttrType[y] = attrType[_outputColumnsIndexes[y] - 1];
//                                if(reqAttrType[y].attrType == AttrType.attrString) {
//                                    s_sizes[j] = _s_sizes[_outputColumnsIndexes[y] - 1];
//                                    j++;
//                                }
//                            }
//                            short[] strSizes = Arrays.copyOfRange(s_sizes, 0, j);
//
//                            Tuple tuple = new Tuple();
//                            try {
//                                tuple.setHdr((short) numOfOutputColumns, reqAttrType, strSizes);
//                            } catch (InvalidTypeException e) {
//                                e.printStackTrace();
//                            }
//
//                            for(int k=0; k<numOfOutputColumns; k++){
//                                int indexNumber = _outputColumnsIndexes[k];
//                                Heapfile heapfile = cf.getColumnFiles()[indexNumber-1];
//                                Tuple tupleTemp = Util.getTupleFromPosition(positions[0], heapfile);
//                                tupleTemp.initHeaders();
//                                if(attrType[indexNumber-1].attrType == AttrType.attrString) {
//                                    tuple.setStrFld(k+1, tupleTemp.getStrFld(1));
//                                }else if(attrType[indexNumber-1].attrType == AttrType.attrInteger) {
//                                    tuple.setIntFld(k+1, tupleTemp.getIntFld(1));
//                                }else if(attrType[indexNumber-1].attrType == AttrType.attrReal) {
//                                    tuple.setFloFld(k+1, tupleTemp.getFloFld(1));
//                                }
//                            }
//                            return tuple;
//                        }
//                    }
//                    //return Jtuple;
//               // }
//

        }
        return null;

    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {

    }


}
