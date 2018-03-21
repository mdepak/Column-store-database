package columnar;

import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import java.io.IOException;

enum FileType
{
  DATA_FILE, BTREE_FILE, BITMAP_FILE;

}

public class ColumnarHeaderRecord {
  FileType fileType;
  String fileName; // TODO: Assume filename of columnar file, btree, bitmap as fixed values
  ValueClass valueClass;
  int maxValSize;
  int columnNo;
  AttrType attrType;


  private AttrType[] attrTypes;
  private short[] strSizes;

  public ColumnarHeaderRecord(FileType fileType, int columnNo, AttrType attrType, String fileName, ValueClass valueClass,
      int maxValSize) {
    this.fileType = fileType;
    this.columnNo = columnNo;
    this.attrType = attrType;
    this.fileName = fileName;
    this.valueClass = valueClass;
    this.maxValSize = maxValSize;
  }


  Tuple getTuple()
      throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException {
    attrTypes = new AttrType[4];

    attrTypes[0] = new AttrType(AttrType.attrInteger);// File type
    attrTypes[1] = new AttrType(AttrType.attrInteger);// Column Number
    attrTypes[2] = new AttrType(AttrType.attrInteger);// Attribue Type
    //TODO: Assumes that the file name field is 30 characters
    attrTypes[3] = new AttrType(AttrType.attrString); // File name

    //TODO: Save value also to the header file - for seraching in case of bitmap files
    strSizes = new short[1];
    strSizes[0] = 30;

    int tupleSize = 4+ 4 + 4+ 30;


    Tuple t = new Tuple(tupleSize);
    t.setHdr((short) 4, attrTypes, strSizes);
    int size = t.size();


    Tuple tuple = new Tuple(size);
    tuple.setHdr((short) 4, attrTypes, strSizes);

    tuple.setIntFld(1, fileType.ordinal());
    tuple.setIntFld(2, columnNo);
    tuple.setIntFld(3, attrType.attrType);
    tuple.setStrFld(4, fileName);

    return tuple;
  }
}
