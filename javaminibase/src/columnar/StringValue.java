package columnar;

import btree.KeyClass;
import btree.StringKey;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;
import java.io.IOException;

public class StringValue extends ValueClass {

  String val;

  StringValue() {
  }

  public StringValue(String val) {
    this.val = val;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StringValue that = (StringValue) o;

    return val.equals(that.val);
  }

  @Override

  public int hashCode() {
    return val != null ? val.hashCode() : 0;
  }

  @Override
  public Object getValue() {
    return val;
  }

  @Override
  public void setValue(Object val) {
    this.val = (String) val;
  }

  @Override
  public void setValueFromColumnTuple(Tuple columnarTuple, int fieldPos)
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

  @Override
  void setValueFromRowTuple(Tuple rowTuple, int fieldPos)
      throws IOException, FieldNumberOutOfBoundException {
    val = rowTuple.getStrFld(fieldPos);
  }

  @Override
  public Boolean evaluateEquals(ValueClass otherValue) {
    if (this == otherValue) {
      return true;
    }
    if (otherValue == null || getClass() != otherValue.getClass()) {
      return false;
    }

    StringValue that = (StringValue) otherValue;

    return val.compareTo(that.val) == 0;
  }

  @Override
  public Boolean evaluateGT(ValueClass otherValue) {
    if (this == otherValue) {
      return true;
    }
    if (otherValue == null || getClass() != otherValue.getClass()) {
      return false;
    }

    StringValue that = (StringValue) otherValue;

    return val.compareTo(that.val) > 0;
  }

  @Override
  public Boolean evaluateGTEquals(ValueClass otherValue) {
    if (this == otherValue) {
      return true;
    }
    if (otherValue == null || getClass() != otherValue.getClass()) {
      return false;
    }

    StringValue that = (StringValue) otherValue;

    return val.compareTo(that.val) >= 0;
  }

  @Override
  public Boolean evaluateLT(ValueClass otherValue) {
    if (this == otherValue) {
      return true;
    }
    if (otherValue == null || getClass() != otherValue.getClass()) {
      return false;
    }

    StringValue that = (StringValue) otherValue;

    return val.compareTo(that.val) < 0;
  }

  @Override
  public Boolean evaluateLTEquals(ValueClass otherValue) {
    if (this == otherValue) {
      return true;
    }
    if (otherValue == null || getClass() != otherValue.getClass()) {
      return false;
    }

    StringValue that = (StringValue) otherValue;

    return val.compareTo(that.val) <= 0;
  }

  @Override
  public Boolean evaluateNotEquals(ValueClass otherValue) {
    if (this == otherValue) {
      return true;
    }
    if (otherValue == null || getClass() != otherValue.getClass()) {
      return false;
    }

    StringValue that = (StringValue) otherValue;

    return val.compareTo(that.val) != 0;
  }

  @Override
  public String toString() {
    return val;
  }
}
