package columnar;

import btree.KeyClass;
import btree.StringKey;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import java.io.IOException;

public class StringValue extends ValueClass {

  String val;

  @Override
  Object getValue() {
    return val;
  }

  @Override
  void setValue(Object val) {
    this.val = (String) val;
  }

  @Override
  void setValueFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException {
    String str = columnarTuple.getStrFld(fieldPos);
    this.val = str;
  }

  @Override
  void setValueinRowTuple(Tuple rowTuple, int fieldNo)
      throws IOException, FieldNumberOutOfBoundException {
    rowTuple.setStrFld(fieldNo, val);
  }

  @Override
  KeyClass getKeyClassFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException {
    String str = columnarTuple.getStrFld(fieldPos);
    return new StringKey(str);
  }
}
