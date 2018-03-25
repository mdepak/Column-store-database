package index;

import bitmap.BitMapFile;
import btree.*;
import columnar.Columnarfile;
import columnar.StringValue;
import columnar.Util;
import columnar.ValueClass;
import global.AttrType;
import global.IndexType;
import global.PageId;
import global.RID;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Index Scan iterator will directly access the required tuple using the provided key. It will also
 * perform selections and projections. information about the tuples and the index are passed to the
 * constructor, then the user calls <code>get_next()</code> to get the tuples.
 */
public class ColumnIndexScan extends Iterator {

    /**
     * class constructor. set up the index scan.
     *
     * @param index type of the index (B_Index, Hash)
     * @param relName name of the input relation
     * @param indName name of the input index
     * @param types array of types in this relation
     * @param str_sizes array of string sizes (for attributes that are string)
     * @param noInFlds number of fields in input tuple
     * @param noOutFlds number of fields in output tuple
     * @param outFlds fields to project
     * @param selects conditions to apply, first one is primary
     * @param fldNum field number of the indexed field
     * @param indexOnly whether the answer requires only the key or the tuple
     * @throws IndexException error from the lower layer
     * @throws InvalidTypeException tuple type not valid
     * @throws InvalidTupleSizeException tuple size not valid
     * @throws UnknownIndexTypeException index type unknown
     * @throws IOException from the lower layer
     */
    public FldSpec[] perm_mat;
    private String _relName;
    private String _indName;
    private IndexFile indFile;
    private BitMapFile bitMapFile;
    private IndexFileScan indScan;
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
    private int _fldNum;
    private boolean index_only;
    private Scan bitMapScan;

    public ColumnIndexScan(
            IndexType index,
            final String relName,
            final String indName,
            AttrType types[],
            short str_sizes[],
            int outputColumnsIndexes[],
            CondExpr selects[],
            final boolean indexOnly
    )
            throws IndexException,
            UnknownIndexTypeException {

        _relName = relName;
        _indName = indName;
        _types = types;
        _s_sizes = str_sizes;
        _outputColumnsIndexes = outputColumnsIndexes;

        short[] ts_sizes;
        Jtuple = new Tuple();

        t1_size = tuple1.size();
        index_only = indexOnly;  // added by bingjie miao

        try {
            f = new Heapfile(relName);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch (index.indexType) {
            // linear hashing is not yet implemented
            case IndexType.B_Index:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indFile = new BTreeFile(indName);
                } catch (Exception e) {
                    throw new IndexException(e,
                            "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }
                try {
                    indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
                } catch (Exception e) {
                    throw new IndexException(e,
                            "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;

            case IndexType.BIT_MAP:
                try {
//          bitMapFile = new BitMapFile(indName);
          /* String matchingValueStr;
          String unmatchingValueStr;
          int numOfConstraints = _selects.length;
          int numOfConstraints = 1;
          for(int i=0; i<numOfConstraints; i++) {
            int columnNumber = 1;
            Columnarfile cf = new Columnarfile(relName);
            List<columnar.ColumnarHeaderRecord> chr = cf.bitmapIndexes.get(columnNumber);
            if (_selects[i].type1.toString() != _selects[i].type2.toString()) {
              throw new Exception("Operand Attribute mismatch error");
            } else {
              if (_selects[i].op.toString() == "aopEQ") {
                if (_selects[i].type1.toString() == _selects[i].type2.toString() && _selects[i].type2.toString() == "attrString") {
                  matchingValueStr = _selects[i].operand2.string;
                  List<String> listOfFiles = new ArrayList<String>();
                  Iterator<String> itr = al.iterator();
                  while (itr.hasNext()) {
                    String element = itr.next();
                    System.out.print(element + " ");
                  }
                }
              } else if (_selects[i].op.toString() == "aopLT") {
              } else if (_selects[i].op.toString() == "aopGT") {
              } else if (_selects[i].op.toString() == "aopNE") {
              } else if (_selects[i].op.toString() == "aopLE") {
              } else if (_selects[i].op.toString() == "aopGE") {
              }
            }
          } */
                    String bitMapIndexFileName = indName + ".";
                    if (_selects[0].type2.toString() == "attrString") {
                        bitMapIndexFileName += _selects[0].operand2.string;
                    } else if (_selects[0].type2.toString() == "attrInteger") {
                        bitMapIndexFileName += _selects[0].operand2.integer;
                    } else if (_selects[0].type2.toString() == "attrReal") {
                        bitMapIndexFileName += _selects[0].operand2.real;
                    }
                    if (bitMapIndexFileName.charAt(bitMapIndexFileName.length() - 1) == '.') {
                        throw new Exception("Attribute Type Error in Value Constraints");
                    } else {
                        bitMapFile = new BitMapFile(bitMapIndexFileName);
                    }
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BitMapFile exceptions caught from BitMapFile constructor");
                }
                try {
                    f = new Heapfile(indName);
                    bitMapScan = f.openScan();
                } catch (Exception e) {
                    throw new IndexException(e,
                            "IndexScan.java: exception caught from Heapfile during bitmap scan.");
                }
                break;

            case IndexType.None:

            default:
                throw new UnknownIndexTypeException("Only BTree and BitMap index is supported so far");

        }

    }

    /**
     * returns the next tuple. if <code>index_only</code>, only returns the key value (as the first
     * field in a tuple) otherwise, retrive the tuple and returns the whole tuple
     *
     * @return the tuple
     * @throws IndexException          error from the lower layer
     * @throws UnknownKeyTypeException key type unknown
     * @throws IOException             from the lower layer
     */
    public Tuple get_next()
            throws IndexException,
            UnknownKeyTypeException,
            IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException {
        RID rid = null;
        int unused;
        KeyDataEntry nextentry = null;

        if (indFile != null) {
            try {
                nextentry = indScan.get_next();
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: BTree error");
            }

            while (nextentry != null) {
                if (index_only) {
                    // only need to return the key
                    AttrType[] attrType = new AttrType[1];
                    short[] s_sizes = new short[1];
                    if (_types[_fldNum - 1].attrType == AttrType.attrInteger) {
                        attrType[0] = new AttrType(AttrType.attrInteger);
                        try {
                            Jtuple.setHdr((short) 1, attrType, s_sizes);
                        } catch (Exception e) {
                            throw new IndexException(e, "IndexScan.java: Heapfile error");
                        }
                        try {
                            Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey().intValue());
                        } catch (Exception e) {
                            throw new IndexException(e, "IndexScan.java: Heapfile error");
                        }
                    } else if (_types[_fldNum - 1].attrType == AttrType.attrString) {
                        attrType[0] = new AttrType(AttrType.attrString);
                        // calculate string size of _fldNum
                        int count = 0;
                        for (int i = 0; i < _fldNum; i++) {
                            if (_types[i].attrType == AttrType.attrString) {
                                count++;
                            }
                        }
                        s_sizes[0] = _s_sizes[count - 1];
                        try {
                            Jtuple.setHdr((short) 1, attrType, s_sizes);
                        } catch (Exception e) {
                            throw new IndexException(e, "IndexScan.java: Heapfile error");
                        }
                        try {
                            Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
                        } catch (Exception e) {
                            throw new IndexException(e, "IndexScan.java: Heapfile error");
                        }
                    } else {
                        // attrReal not supported for now
                        throw new UnknownKeyTypeException("Only Integer and String keys are supported so far");
                    }
                    return Jtuple;
                } else {
                    Columnarfile cf = new Columnarfile(_relName);
                    int columnNumber = cf.getColumnNumber(_indName);
                    Heapfile hf = new Heapfile(_relName + "." + columnNumber);

                    // not index_only, need to return the whole tuple
                    rid = ((LeafData) nextentry.data).getData();
                    int position = getPositionFromRID(rid, hf);
                    int numOfOutputColumns = _outputColumnsIndexes.length;
                    RID[] outputRIDs = new RID[numOfOutputColumns];
                    Tuple outputTuple = new Tuple();
                    AttrType[] attrTypes = cf.getType();
                    Scan[] scans = selectiveTupleScan(cf, _outputColumnsIndexes);
                    ValueClass[] values = new ValueClass[numOfOutputColumns];
                    AttrType[] attrType = new AttrType[numOfOutputColumns];
                    short[] s_sizes2 = new short[numOfOutputColumns];

                    int columns = cf.getNumColumns();
                    int j = 0;
                    for (int i = 0; i < numOfOutputColumns; i++) {
                        attrType[i] = _types[_outputColumnsIndexes[i]];
                        //TODO: Fix the compilation error and uncomment
//                        if(attrType.toString == "attrString") {
//                            s_sizes2[j] = _s_sizes[i];
//                            j++;
//                        }
                    }
                    //TODO: Fix the compilation error and uncomment
                    short[] s_sizes = new short[0];
                    //short[] s_sizes = Arrays.copyOf(s_sizes2, 0, j-1);
                    try {
                        outputTuple.setHdr((short) numOfOutputColumns, attrType, s_sizes);
                    } catch (Exception e) {
                        System.out.println("set tuple header exception in getNext function");
                    }

                    for (int i = 0; i < numOfOutputColumns; i++) {
                        int indexNumber = _outputColumnsIndexes[i];
                        //TODO: Fix the compilation error and uncomment
                        //outputRIDs[i] = getRIDFromPosition(position, indexNumber);
                        Scan scan = scans[indexNumber];
                        outputTuple = scan.getNext(outputRIDs[i]);
                        if (outputTuple == null) {
                            return null;
                        }
                        outputTuple.initHeaders();
                        ValueClass value = Util.valueClassFactory(attrTypes[indexNumber]);
                        value.setValueFromColumnTuple(outputTuple, i + 1);
                        System.out.println(value.getValue());
                    }
                    return outputTuple;
                }
            }
        }
        return null;
    }

    /**
     * Cleaning up the index scan, does not remove either the original relation or the index from the
     * database.
     *
     * @throws IndexException error from the lower layer
     * @throws IOException    from the lower layer
     */
    public void close() throws IOException, IndexException {
        if (!closeFlag) {
            if (indScan instanceof BTFileScan) {
                try {
                    ((BTFileScan) indScan).DestroyBTreeFileScan();
                } catch (Exception e) {
                    throw new IndexException(e, "BTree error in destroying index scan.");
                }
            } else if (bitMapFile != null) {
                try {
                    bitMapFile.destroyBitMapFile();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            closeFlag = true;
        }
    }

    public Scan[] selectiveTupleScan(Columnarfile cf, int[] outputColumnIndexes) {
        int columns = cf.getNumColumns();
        Heapfile[] columnFiles = cf.getColumnFiles();
        Scan[] scans = new Scan[outputColumnIndexes.length];
        int outputColumnsLength = outputColumnIndexes.length;
        boolean found = false;

        for (int idx = 0; idx<outputColumnsLength; idx++) {
            try {
                scans[idx] = new Scan(columnFiles[outputColumnIndexes[idx] - 1]);
            } catch (Exception e) {
                System.out.println("Selective Tuple Scan : scan exception");
            }
        }
        return scans;
    }


    public int getPositionFromRID(RID rid, Heapfile hf) throws HFBufMgrException, IOException, InvalidSlotNumberException, InvalidTupleSizeException {
        boolean flag = true;
        PageId currentDirPageId = new PageId(hf._firstDirPageId.pid);
        HFPage currentDirPage = new HFPage();

        int currPosition = 0;
        PageId nextDirPageId = new PageId(0);

        while (currentDirPageId.pid != hf.INVALID_PAGE && flag) {
            hf.pinPage(currentDirPageId, currentDirPage, false);
            RID recid = new RID();
            Tuple atuple;
            for (recid = currentDirPage.firstRecord();
                 recid != null;  // rid==NULL means no more record
                 recid = currentDirPage.nextRecord(recid)) {
                atuple = currentDirPage.getRecord(recid);
                DataPageInfo dpinfo = new DataPageInfo(atuple);

                if (rid.pageNo.pid == dpinfo.getPageId().pid) {
                    currPosition += rid.slotNo;
                    flag = false;
                } else {
                    currPosition += dpinfo.recct;
                }
            }
            // ASSERTIONS: no more record
            // - we have read all datapage records on
            //   the current directory page.

            if (flag) {
                nextDirPageId = currentDirPage.getNextPage();
                hf.unpinPage(currentDirPageId, false /*undirty*/);
                currentDirPageId.pid = nextDirPageId.pid;
            }
        }
        return currPosition;
    }
}
