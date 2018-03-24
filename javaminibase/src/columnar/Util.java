package columnar;

import static global.AttrType.attrInteger;
import static global.AttrType.attrNull;
import static global.AttrType.attrReal;
import static global.AttrType.attrString;
import static global.AttrType.attrSymbol;

import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import java.io.IOException;

public class Util {

  public static ValueClass valueClassFactory(AttrType type) {
    ValueClass val = null;
    switch (type.attrType) {
      case attrString:
        val = new StringValue();
        break;
      case attrInteger:
        val = new IntegerValue();
        break;
      case attrReal:
        val = new FloatValue();
        break;
      case attrSymbol:
        //TODO: Find out whether this is character and implement it.
        break;
      case attrNull:
        //TODO: Find out the right type and implement it.
        break;
    }

    return val;
  }


  public static Tuple createColumnarTuple(Tuple rowTuple, int fieldNo, AttrType attrType)
      throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException {
    //TODO: Init tuple with specific size rather than default 1024
    Tuple columnTuple = new Tuple();
    short colTupFields = 1;
    switch (attrType.attrType) {
      case attrString:
        String strVal = rowTuple.getStrFld(fieldNo);
        columnTuple
            .setHdr(colTupFields, new AttrType[]{attrType}, new short[]{(short) strVal.length()});
        columnTuple.setStrFld(1, strVal);
        break;
      case attrInteger:
        int intVal = rowTuple.getIntFld(fieldNo);
        columnTuple.setHdr(colTupFields, new AttrType[]{attrType}, new short[]{});
        columnTuple.setIntFld(1, intVal);
        break;
      case attrReal:
        float floatVal = rowTuple.getFloFld(fieldNo);
        columnTuple.setHdr(colTupFields, new AttrType[]{attrType}, new short[]{});
        columnTuple.setFloFld(1, floatVal);
        break;
      case attrSymbol:
        //TODO: Find out whether this is character and implement it.
        break;
      case attrNull:
        //TODO: Find out the right type and implement it.
        break;
    }

    return columnTuple;
  }

  public static void printBitsInByte(byte[] val)
  {
    for(int i =0 ; i<val.length; i++)
    {
    System.out.println(Integer.toBinaryString( (int) val[i]));
    }
  }
}
