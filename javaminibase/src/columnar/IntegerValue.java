package columnar;

import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import java.io.IOException;

public class IntegerValue extends ValueClass {

  Integer val;

  @Override
  Object getValue() {
    return val;
  }

  @Override
  void setValue(Object val) {
    this.val = (Integer) val;
  }

  @Override
  void setValueFromColumnTuple(Tuple columnTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException {
    int val = columnTuple.getIntFld(fieldPos);
    this.val = val;
  }

  @Override
  void setValueinRowTuple(Tuple rowTuple, int fieldNo)
      throws IOException, FieldNumberOutOfBoundException {
    rowTuple.setIntFld(fieldNo, val.intValue());
  }
}
