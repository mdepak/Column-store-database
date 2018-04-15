package columnar;

import btree.KeyClass;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import java.io.IOException;

public abstract class ValueClass {

  abstract public Object getValue();

  abstract public void setValue(Object val);

  public abstract void setValueFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException;

  // Should ensure the headers are specified.. otherwise this will not work
  abstract void setValueinRowTuple(Tuple rowTuple, int fieldNo)
      throws IOException, FieldNumberOutOfBoundException;

  abstract KeyClass getKeyClassFromColumnTuple(Tuple columnarTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException;

  abstract void setValueFromRowTuple(Tuple rowTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException;


  //TODO: Uncomment and implement these methods in the corresponding sub classes


  public abstract Boolean evaluateEquals(ValueClass otherValue);

  public abstract Boolean evaluateGT(ValueClass otherValue);

  public abstract Boolean evaluateGTEquals(ValueClass otherValue);

  public abstract Boolean evaluateLT(ValueClass otherValue);

  public abstract Boolean evaluateLTEquals(ValueClass otherValue);

  public abstract Boolean evaluateNotEquals(ValueClass otherValue);
}
