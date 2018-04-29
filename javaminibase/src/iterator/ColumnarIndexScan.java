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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import tests.Util;

import java.io.IOException;

public class ColumnarIndexScan extends Iterator {

    private Columnarfile columnarfile;
    private CondExpr[] _selects;
    private FldSpec[] _outFlds;
    private Iterator[] iterators;
    private List<Heapfile> heapFiles;
    public ColumnarIndexScan(String relName, IndexType[] index, FldSpec[] outFlds, CondExpr[] selects) throws Exception {

        _selects = selects;
        _outFlds = outFlds;
        try {

            heapFiles = new ArrayList<>();
            columnarfile = new Columnarfile(relName);
            iterators = new Iterator[10];
            int cnt = 0;
            for (int i = 0; i < selects.length; i++) {
                CondExpr condExpr = selects[i];
                while (condExpr != null) {
                    int colNum = condExpr.operand1.symbol.offset;
                    CondExpr[] newExprArr = new CondExpr[2];
                    CondExpr newExpr = new CondExpr(condExpr);
                    newExprArr[0] = newExpr;
                    newExprArr[0].next = null;
                    newExprArr[1] = null;
                    AttrType[] attrType = new AttrType[1];
                    attrType[0] = columnarfile.getType()[colNum - 1];
                    short[] strsizes = new short[1];
                    strsizes[0] = (short) 100;
                    String heapName = relName + "." + colNum;
                    int[] selectedCols = new int[1];
                    FldSpec[] _outFlds = new FldSpec[1];
                    RelSpec rel = new RelSpec(RelSpec.outer);
                    _outFlds[0] = new FldSpec(rel, 1);
                    switch (index[colNum - 1].indexType) {
                        case IndexType.B_Index:
                            String indName = "BTree" + relName + colNum;
                            AttrType[] type = new AttrType[1];
                            type[0] = new AttrType(AttrType.attrInteger);
                            ColumnIndexScan columnIndexScan = new ColumnIndexScan(new IndexType(IndexType.B_Index), relName, heapName, indName, attrType, strsizes, 1, null, newExprArr, false);
                            int position = 0;
                            Heapfile heapfile = new Heapfile(null);
                            heapFiles.add(heapfile);
                            do {
                                Tuple tuple = new Tuple();
                                position = columnIndexScan.get_next_pos();
                                if(position != -1){
                                    tuple.setHdr((short) 1, type, strsizes);
                                    tuple.setIntFld(1, position+1);
                                    heapfile.insertRecord(tuple.getTupleByteArray());
                                    System.out.println(position+1);
                                }
                            }while((position != -1));

                            FileScan fscan = new FileScan(heapfile._fileName, type, strsizes, (short) 1, 1, _outFlds, null);
                            Sort sort = new Sort(type, (short) 1, strsizes, fscan, 1, new TupleOrder(TupleOrder.Ascending), 4, 3, true);
                            iterators[cnt] = sort;
                            cnt++;
                            break;
                        case IndexType.BIT_MAP:
                            selectedCols[0] = 1;
                            iterators[cnt] = new BitmapIterator(relName, selectedCols, newExprArr);
                            cnt++;
                            break;
                        case IndexType.None:
                            selectedCols[0] = 1;
                            ColumnarFileScan columnarFileScan = new ColumnarFileScan(relName, heapName, attrType, strsizes, (short) 1, 1, selectedCols, _outFlds, newExprArr, false);
                            iterators[cnt] = columnarFileScan;
                            cnt++;
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
    public Tuple get_next() throws Exception {

        try {
            while(true) {
                int cnt = 0;
                HashSet<Integer> positions = new HashSet<>();
                for (int i = 0; i < _selects.length - 1; i++) {
                    CondExpr condExpr = _selects[i];
                    Integer min_pos = Integer.MAX_VALUE;
                    while (condExpr != null) {
                        int pos = iterators[cnt].get_next_pos();
                        cnt++;
                        min_pos = Math.min(min_pos, pos);
                        condExpr = condExpr.next;
                    }
                    if(min_pos == -1) return null;
                    positions.add(min_pos);
                }
                if(positions.size() == 1){

                    Tuple rowTuple = new Tuple();
                    AttrType[] attrType = columnarfile.getType();
                    for(int i=0; i< _outFlds.length; i++){
                        int indexNumber = _outFlds[i].offset;
                        Heapfile heapfile = columnarfile.getColumnFiles()[indexNumber-1];
                        Tuple tupleTemp = columnar.Util.getTupleFromPosition(positions.iterator().next(), heapfile);
                        tupleTemp.initHeaders();
                        if(attrType[indexNumber-1].attrType == AttrType.attrString) {
                            rowTuple.setStrFld(i+1, tupleTemp.getStrFld(1));
                        }else if(attrType[indexNumber-1].attrType == AttrType.attrInteger) {
                            rowTuple.setIntFld(i+1, tupleTemp.getIntFld(1));
                        }else if(attrType[indexNumber-1].attrType == AttrType.attrReal) {
                            rowTuple.setFloFld(i+1, tupleTemp.getFloFld(1));
                        }
                    }
                    return rowTuple;
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int get_next_pos() throws  Exception {
        return 0;
    }

    @Override
    public void close() {

        try {
            for (Iterator iterator : iterators) iterator.close();

            for (Heapfile heapFile : heapFiles) {
                heapFile.deleteFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
