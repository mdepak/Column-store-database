package iterator;

import columnar.BitmapIterator;
import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import global.TupleOrder;
import heap.Heapfile;
import heap.InvalidTypeException;
import heap.Tuple;
import index.ColumnIndexScan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import javafx.util.Pair;

public class ColumnarIndexScan extends Iterator {

    private Columnarfile columnarfile;
    private CondExpr[] _selects;
    private FldSpec[] _outFlds;
    private Iterator[] iterators;
    private List<String> heapFiles;
    private AttrType[] colTypes;
    private short[] strSizes;
    boolean first = true;
    int andCount = 0;
    List<Integer> orCount;
    List<List<Pair<Integer, Iterator>>> posCache;
    public ColumnarIndexScan(String relName, IndexType[] index, FldSpec[] outFlds, CondExpr[] selects) throws Exception {

        _selects = selects;
        _outFlds = outFlds;
        try {

            int cnt = 0;
            heapFiles = new ArrayList<>();
            columnarfile = new Columnarfile(relName);
            colTypes = columnarfile.getType();
            strSizes = columnarfile.getStrSizes();
            iterators = new Iterator[10];
            orCount = new ArrayList<>();
            andCount = _selects.length - 1;
            for (int i = 0; i < selects.length; i++) {
                CondExpr condExpr = selects[i];
                int orCC = 0;
                while (condExpr != null)
                {
                    orCC++;
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
                            heapFiles.add(heapfile._fileName);
                            do {
                                Tuple tuple = new Tuple();
                                position = columnIndexScan.get_next_pos();
                                if(position != -1){
                                    tuple.setHdr((short) 1, type, strsizes);
                                    tuple.setIntFld(1, position);
                                    heapfile.insertRecord(tuple.getTupleByteArray());
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
                orCount.add(orCC);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple get_next() throws Exception {

        try {
            while(true) {

                if(first){
                    int counter = 0;
                    posCache = new ArrayList<>();
                    for(int i=0; i<andCount; i++){
                        List<Pair<Integer, Iterator>> rowCache = new ArrayList<>();
                        for(int j=0; j<orCount.get(i); j++){
                            int tempPos = iterators[counter].get_next_pos();
                            if (tempPos == -1)
                                rowCache.add(new Pair<>(Integer.MAX_VALUE, iterators[counter]));
                            else
                                rowCache.add(new Pair<>(tempPos, iterators[counter]));
                            counter++;
                        }
                        posCache.add(rowCache);
                    }
                    first = false;
                }

                HashSet<Integer> andPos = new HashSet<>();
                int globalMin = Integer.MAX_VALUE;
                for(int i=0; i<andCount; i++) {
                    List<Pair<Integer, Iterator>> rowCache = posCache.get(i);
                    int min_pos = Integer.MAX_VALUE;
                    for(int j=0; j<orCount.get(i); j++){
                        int pos = rowCache.get(j).getKey();
                        if(pos != -1 && pos < min_pos) {
                            min_pos = pos;
                        }
                    }
                    if(min_pos == Integer.MAX_VALUE) return null;
                    globalMin = Integer.min(globalMin, min_pos);
                    andPos.add(min_pos);
                }
                for(int i=0; i<andCount; i++){
                    List<Pair<Integer, Iterator>> tempCache = posCache.get(i);
                    for(int j=0; j<orCount.get(i); j++){
                        if(tempCache.get(j).getKey() == globalMin ){
                            int newpos = tempCache.get(j).getValue().get_next_pos();
                            Iterator itr = tempCache.get(j).getValue();
                            tempCache.set(j, new Pair<>(newpos, itr));
                        }
                    }
                }
                if(andPos.size() == 1){

                    Tuple rowTuple = new Tuple();
                    try {
                        rowTuple.setHdr((short) _outFlds.length, colTypes, strSizes);
                    } catch (InvalidTypeException e) {
                        e.printStackTrace();
                    }
                    AttrType[] attrType = columnarfile.getType();
                    for(int i=0; i< _outFlds.length; i++){
                        int indexNumber = _outFlds[i].offset;
                        Heapfile heapfile = columnarfile.getColumnFiles()[indexNumber-1];
                        Tuple tupleTemp = columnar.Util.getTupleFromPosition(andPos.iterator().next(), heapfile);
                        tupleTemp.initHeaders();
                        if(attrType[indexNumber-1].attrType == AttrType.attrString) {
                            rowTuple.setStrFld(i+1, tupleTemp.getStrFld(1));
                            System.out.print(rowTuple.getStrFld(i+1));
                        }else if(attrType[indexNumber-1].attrType == AttrType.attrInteger) {
                            rowTuple.setIntFld(i+1, tupleTemp.getIntFld(1));
                            System.out.print(rowTuple.getIntFld(i+1));
                        }else if(attrType[indexNumber-1].attrType == AttrType.attrReal) {
                            rowTuple.setFloFld(i+1, tupleTemp.getFloFld(1));
                            System.out.print(rowTuple.getFloFld(i+1));
                        }
                        System.out.print("\t");
                    }
                    System.out.println("");
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
            for (int i=0; i< andCount; i++) iterators[i].close();

            for (String name : heapFiles) {
                Heapfile heapfile = new Heapfile(name);
                heapfile.deleteFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
