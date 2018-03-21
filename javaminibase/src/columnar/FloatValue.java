package columnar;

import btree.IntegerKey;
import btree.KeyClass;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import java.io.IOException;

public class FloatValue extends ValueClass {

  Float val;

  @Override
  Object getValue() {
    return val;
  }

  @Override
  void setValue(Object val) {
    this.val = (Float) val;
  }

  @Override
  void setValueFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException {
    float fltVal = columnarTuple.getFloFld(fieldPos);
    this.val = fltVal;
  }

  @Override
  void setValueinRowTuple(Tuple rowTuple, int fieldNo)
      throws IOException, FieldNumberOutOfBoundException {
    rowTuple.setFloFld(fieldNo, val.floatValue());
  }

  @Override
  KeyClass getKeyClassFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException {
    float fltVal = columnarTuple.getFloFld(fieldPos);
    return new IntegerKey((int) fltVal);
  }
}
