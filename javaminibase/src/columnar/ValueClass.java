package columnar;

import btree.KeyClass;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import java.io.IOException;

public abstract class ValueClass {

  abstract public Object getValue();

  abstract void setValue(Object val);

  abstract void setValueFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException;

  // Should ensure the headers are specified.. otherwise this will not work
  abstract void setValueinRowTuple(Tuple rowTuple, int fieldNo)
      throws IOException, FieldNumberOutOfBoundException;

  abstract KeyClass getKeyClassFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException;
}
