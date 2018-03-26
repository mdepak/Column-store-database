package iterator;


import bufmgr.PageNotReadException;
import columnar.Columnarfile;
import columnar.Util;
import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import heap.DataPageInfo;
import heap.FieldNumberOutOfBoundException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * open a heapfile and according to the condition expression to get output file, call get_next to
 * get all tuples
 */
public class ColumnarFileScan extends Iterator {

  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes;
  private Heapfile f;
  private Scan scan;
  private Tuple tuple1;
  private Tuple Jtuple;
  private int t1_size;
  private int nOutFlds;
  private CondExpr[] OutputFilter;
  public FldSpec[] perm_mat;
  private int[] selectedCols;
  private boolean _fileaccess;
  private String _columnfile;
  private String relname;
  int rowpos;


  /**
   * corowposnstructor
   *
   * @param file_name heapfile to be opened
   * @param in1[] array showing what the attributes of the input fields are.
   * @param s1_sizes[] shows the length of the string fields.
   * @param len_in1 number of attributes in the input tuple
   * @param n_out_flds number of fields in the out tuple
   * @param proj_list shows what input fields go where in the output tuple
   * @param outFilter select expressions
   * @throws IOException some I/O fault
   * @throws FileScanException exception from this class
   * @throws TupleUtilsException exception from this class
   * @throws InvalidRelation invalid relation
   */
  public ColumnarFileScan(String columnfile,
      String file_name,
      AttrType in1[],
      short s1_sizes[],
      short len_in1,
      int n_out_flds,
      int[] selectedCols,
      FldSpec[] proj_list,
      CondExpr[] outFilter,
      boolean fileScan
  )
      throws IOException,
      FileScanException,
      TupleUtilsException,
      InvalidRelation {
    _in1 = in1;
    in1_len = len_in1;
    s_sizes = s1_sizes;
    _columnfile = columnfile;
    relname = file_name;

    Jtuple = new Tuple();
    AttrType[] Jtypes = new AttrType[n_out_flds];
    short[] ts_size;
    ts_size = TupleUtils
        .setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);

    OutputFilter = outFilter;
    perm_mat = proj_list;
    nOutFlds = n_out_flds;
    tuple1 = new Tuple();
    this.selectedCols = selectedCols;
    this._fileaccess = fileScan;

    try {
      tuple1.setHdr(in1_len, _in1, s1_sizes);
    } catch (Exception e) {
      throw new FileScanException(e, "setHdr() failed");
    }
    t1_size = tuple1.size();

    try {
      f = new Heapfile(file_name);

    } catch (Exception e) {
      throw new FileScanException(e, "Create new heapfile failed");
    }

    try {
      scan = f.openScan();
    } catch (Exception e) {
      throw new FileScanException(e, "openScan() failed");
    }
  }

  /**
   * @return shows what input fields go where in the output tuple
   */
  public FldSpec[] show() {
    return perm_mat;
  }

  /**
   * @return the result tuple
   * @throws JoinsException some join exception
   * @throws IOException I/O errors
   * @throws InvalidTupleSizeException invalid tuple size
   * @throws InvalidTypeException tuple type not valid
   * @throws PageNotReadException exception from lower layer
   * @throws PredEvalException exception from PredEval class
   * @throws UnknowAttrType attribute type unknown
   * @throws FieldNumberOutOfBoundException array out of bounds
   * @throws WrongPermat exception for wrong FldSpec argument
   */
  public Tuple get_next()
      throws JoinsException,
      IOException,
      InvalidTupleSizeException,
      InvalidTypeException,
      PageNotReadException,
      PredEvalException,
      UnknowAttrType,
      FieldNumberOutOfBoundException,
      WrongPermat {
    RID rid = new RID();
    ;

    while (true) {
      if ((tuple1 = scan.getNext(rid)) == null) {
        return null;
      }

      tuple1.setHdr(in1_len, _in1, s_sizes);
      if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) {
        //Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);

        try {
          Columnarfile cf = new Columnarfile(_columnfile);
          Heapfile hf = new Heapfile(relname);
          AttrType[] attrType = cf.getType();
          int numOfOutputColumns = selectedCols.length;
          AttrType[] reqAttrType = new AttrType[numOfOutputColumns];
          short[] str_sizes = new short[numOfOutputColumns];
          int j = 0;
          for(int i=0; i<numOfOutputColumns; i++) {
            reqAttrType[i] = attrType[selectedCols[i] - 1];
            if(reqAttrType[i].attrType == AttrType.attrString) {
              str_sizes[j] = s_sizes[selectedCols[i] - 1];
              j++;
            }
          }
          short[] strSizes = Arrays.copyOfRange(str_sizes, 0, j);

          Tuple tuple = new Tuple();
          tuple.setHdr((short) numOfOutputColumns, reqAttrType, strSizes);

          for(int i=0; i<numOfOutputColumns; i++){
            int indexNumber = selectedCols[i];
            Heapfile heapfile = cf.getColumnFiles()[indexNumber-1];
            Tuple tupleTemp = Util.getTupleFromPosition(rowpos, heapfile);
            tupleTemp.initHeaders();
            if(attrType[indexNumber-1].attrType == AttrType.attrString) {
              tuple.setStrFld(i+1, tupleTemp.getStrFld(1));
            }else if(attrType[indexNumber-1].attrType == AttrType.attrInteger) {
              tuple.setIntFld(i+1, tupleTemp.getIntFld(1));
            }else if(attrType[indexNumber-1].attrType == AttrType.attrReal) {
              tuple.setFloFld(i+1, tupleTemp.getFloFld(1));
            }
          }
          rowpos++;
          return tuple;
        } catch (Exception e) {
          System.out.println("Failed");
          e.printStackTrace();
        }
      }
      rowpos++;
    }
  }



  /*
  public RID get_next_rid() throws InvalidTupleSizeException, IOException {
    RID rid = new RID();

    while (true) {
      if ((tuple1 = scan.getNext(rid)) == null) {
        return null;
      }

    }
    return rid;
  }

  */

  /**
   * implement the abstract method close() from super class Iterator to finish cleaning up
   */
  public void close() {

    if (!closeFlag) {
      scan.closescan();
      closeFlag = true;
    }
  }

}


