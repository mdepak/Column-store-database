package tests;

import columnar.Columnarfile;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import java.util.ArrayList;
import java.util.List;

public class Util {

  public static CondExpr[] getValueContraint(List<String> valueContraint){
    if(valueContraint.isEmpty())
      return null;

    int operator = getOperator(valueContraint.get(1));
    int column = getColumnNumber(valueContraint.get(0));

    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(operator);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
    expr[0].next = null;

    String value = valueContraint.get(2);
    if (value.matches("\\d*\\.\\d*")) {
      expr[0].type2 = new AttrType(AttrType.attrReal);
      expr[0].operand2.real = Float.valueOf(value);
    }
    else if(value.matches("\\d+")){
      expr[0].type2 = new AttrType(AttrType.attrInteger);
      expr[0].operand2.integer = Integer.valueOf(value);
    }
    else{
      expr[0].type2 = new AttrType(AttrType.attrString);
      expr[0].operand2.string = value;
    }
    expr[1] = null;
    return expr;
  }


  public static int getOperator(String op){

    int operator = AttrOperator.aopEQ;
    switch(op){
      case "=":
        operator = AttrOperator.aopEQ;
        break;

      case "<":
        operator = AttrOperator.aopLT;
        break;

      case "<=":
        operator = AttrOperator.aopLE;
        break;

      case ">":
        operator = AttrOperator.aopGT;
        break;

      case ">=":
        operator = AttrOperator.aopGE;
        break;

      case "!=":
        operator = AttrOperator.aopNE;
        break;

    }
    return operator;
  }


  public static int getColumnNumber(String columnName){

    int column = 1;
    switch(columnName){
      case "A":
        column = 1;
        break;

      case "B":
        column = 2;
        break;

      case "C":
        column = 3;
        break;

      case "D":
        column = 4;
        break;
    }
    return column;
  }


  public static List<RID> getRIDListHeapFile(List<String> valueConstraint, String columnFileName){

    List<RID> ridList = new ArrayList<>();
    try {

      int colnum = Util.getColumnNumber(valueConstraint.get(0));
      String filename = columnFileName + '.' + String.valueOf(colnum);
      Columnarfile columnarFile = new Columnarfile(columnFileName);
      AttrType[] types = columnarFile.getType();

      AttrType[] attrs = new AttrType[1];
      attrs[0] = types[colnum];

      FldSpec[] projlist = new FldSpec[1];
      RelSpec rel = new RelSpec(RelSpec.outer);
      projlist[0] = new FldSpec(rel, 1);

      short[] strsizes = new short[2];
      strsizes[0] = 100;
      strsizes[1] = 100;

      CondExpr[] expr = Util.getValueContraint(valueConstraint);

      ColumnarFileScan columnarFileScan = new ColumnarFileScan(columnFileName, filename, attrs, strsizes, (short) 1, 1, null, projlist, expr, false);
      RID rid = new RID();
      while(rid != null){
        rid = columnarFileScan.get_next_rid();
        ridList.add(rid);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return ridList;

  }

}
