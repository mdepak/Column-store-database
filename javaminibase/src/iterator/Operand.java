package iterator;


import global.AttrType;

public class Operand {

  public FldSpec symbol;
  public String string;
  public int integer;
  public float real;


  public Object getOperandValue(AttrType attrType){
    switch (attrType.attrType)
    {
      case AttrType.attrString:
        return string;
      case AttrType.attrInteger:
        return integer;
      case AttrType.attrReal:
        return real;
    }
    return null;
  }
}
