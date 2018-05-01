package iterator;


import global.AttrType;

public class Operand {

  public FldSpec symbol;
  public String string;
  public int integer;
  public float real;

  public Operand() {
  }

  public Operand(Operand that)
  {
    this.symbol = new FldSpec(that.symbol);
    this.integer = that.integer;
    this.real = that.real;
    this.symbol = that.symbol;
  }

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
