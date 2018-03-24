package index;

import bitmap.BitMapFile;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.IntegerKey;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.PredEval;
import iterator.Projection;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknownKeyTypeException;
import java.io.IOException;
import heap.Scan;
import global.RID;

/**
 * Index Scan iterator will directly access the required tuple using the provided key. It will also
 * perform selections and projections. information about the tuples and the index are passed to the
 * constructor, then the user calls <code>get_next()</code> to get the tuples.
 */
public class ColumnIndexScan extends Iterator{

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
  private IndexFile indFile;
  private BitMapFile bitMapFile;
  private IndexFileScan indScan;
  private AttrType[] _types;
  private short[] _s_sizes;
  private CondExpr[] _selects;
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
      CondExpr selects[],
      final boolean indexOnly
  )
      throws IndexException,
      InvalidTypeException,
      InvalidTupleSizeException,
      UnknownIndexTypeException,
      IOException {
    _types = types;
    _s_sizes = str_sizes;

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
        try{
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
          if(_selects[0].type2.toString() == "attrString") {
            bitMapIndexFileName += _selects[0].operand2.string;
          } else if(_selects[0].type2.toString() == "attrInteger") {
            bitMapIndexFileName += _selects[0].operand2.integer;
          } else if(_selects[0].type2.toString() == "attrReal") {
            bitMapIndexFileName += _selects[0].operand2.real;
          }
          if(bitMapIndexFileName.charAt(bitMapIndexFileName.length()-1) == '.') {
            throw new Exception("Attribute Type Error in Value Constraints");
          } else {
            bitMapFile = new BitMapFile(bitMapIndexFileName);
          }
        } catch (Exception e) {
          throw  new IndexException(e, "IndexScan.java: BitMapFile exceptions caught from BitMapFile constructor");
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
   * @throws IndexException error from the lower layer
   * @throws UnknownKeyTypeException key type unknown
   * @throws IOException from the lower layer
   */
  public Tuple get_next()
      throws IndexException,
      UnknownKeyTypeException,
      IOException {
    RID rid = null;
    int unused;
    KeyDataEntry nextentry = null;

    if(indFile!=null){
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
        }

        // not index_only, need to return the whole tuple
        rid = ((LeafData) nextentry.data).getData();
        try {
          tuple1 = f.getRecord(rid);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: getRecord failed");
        }

        try {
          tuple1.setHdr((short) _noInFlds, _types, _s_sizes);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        boolean eval;
        try {
          eval = PredEval.Eval(_selects, tuple1, null, _types, null);
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        if (eval) {
          // need projection.java
          try {
            Projection.Project(tuple1, _types, Jtuple, perm_mat, _noOutFlds);
          } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
          }

          return Jtuple;
        }

        try {
          nextentry = indScan.get_next();
        } catch (Exception e) {
          throw new IndexException(e, "IndexScan.java: BTree error");
        }
      }

    }

    else if(bitMapFile!=null)
    {
      try {
        Tuple t=bitMapScan.getNext(rid);
        return t;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return null;
  }

  /**
   * Cleaning up the index scan, does not remove either the original relation or the index from the
   * database.
   *
   * @throws IndexException error from the lower layer
   * @throws IOException from the lower layer
   */
  public void close() throws IOException, IndexException {
    if (!closeFlag) {
      if (indScan instanceof BTFileScan) {
        try {
          ((BTFileScan) indScan).DestroyBTreeFileScan();
        } catch (Exception e) {
          throw new IndexException(e, "BTree error in destroying index scan.");
        }
      }
      else if(bitMapFile!=null)
      {
        try {
          bitMapFile.destroyBitMapFile();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      closeFlag = true;
    }
  }

}
