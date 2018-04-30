package tests;

import columnar.Columnarfile;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Util {


  public static String getDatabasePath(String databaseName)
  {
    String dbpath = "/tmp/" + databaseName + System.getProperty("user.name") + ".minibase-db";
    return dbpath;
  }

  private static  boolean isDatabseExists(String columnDBName)
  {
    File file = new File(getDatabasePath(columnDBName));
    return file.exists();
  }

  public static  void createDatabaseIfNotExists(String columnDBName, int bufferSize)
  {
    int diskPages = 8193;

    if(isDatabseExists(columnDBName))
    {
      diskPages = 0;
    }
    SystemDefs sysdef = new SystemDefs(getDatabasePath(columnDBName), diskPages, bufferSize, "Clock");
    PCounter.initialize();
  }

  public static CondExpr[] getCondExprList(String conditions, String outerTableName, String innerTableName, int conditionType){
    String[] disjunctions = conditions.split("&");
    String[] operands = {"!=", "<=", ">=", "<", ">", "="};
    CondExpr[] expr = new CondExpr[disjunctions.length+1];
    for (int j=0; j< disjunctions.length; j++){
      String [] conditionExpr = disjunctions[j].split("\\|");
      List<List<String>> valueConstraints = new ArrayList<>();
      for(int i=0; i<conditionExpr.length; i++) {
        List<String> valueConstraint = new ArrayList<String>();
        for(int o=0; o<operands.length; o++) {
          if(conditionExpr[i].contains(operands[o])) {
            int index = conditionExpr[i].indexOf(operands[o]);
            String colCons = conditionExpr[i].substring(0, index);
            String valCons = conditionExpr[i].substring(index+operands[o].length(), conditionExpr[i].length());
            valueConstraint.add(colCons);
            valueConstraint.add(operands[o]);
            valueConstraint.add(valCons);
          }
        }
        valueConstraints.add(valueConstraint);
      }
      expr[j] = Util.getValueContraint(valueConstraints, outerTableName, innerTableName, conditionType);
    }
    expr[disjunctions.length] = null;
    return expr;
  }

  public static CondExpr getValueContraint(List<List<String>> valueContraints, String outerTableName, String innerTableName, int conditionType){
    CondExpr expr = new CondExpr();
    CondExpr expr_pointer = expr;
    Iterator itr = valueContraints.iterator();
    while(itr.hasNext()) {
      List<String> valueConstraint = (List<String>) itr.next();
      if (valueConstraint.isEmpty())
        return null;

      int operator = getOperator(valueConstraint.get(1));
      expr.op = new AttrOperator(operator);
      expr.type1 = new AttrType(AttrType.attrSymbol);

      if(conditionType == 3) {
        String[] leftOperand = valueConstraint.get(0).split("\\.");
        if(leftOperand[0].equals(outerTableName)) {
          expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), getColumnNumber(leftOperand[1]));
        }else if(leftOperand[0].equals(innerTableName)) {
          expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), getColumnNumber(leftOperand[1]));
        }

        expr.type2 = new AttrType(AttrType.attrSymbol);
        String[] rightOperand = valueConstraint.get(2).split("\\.");
        if(rightOperand[0].equals(outerTableName)) {
          expr.operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), getColumnNumber(rightOperand[1]));
        }else if(rightOperand[0].equals(innerTableName)) {
          expr.operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), getColumnNumber(rightOperand[1]));
        }
      } else {
        if(conditionType == 1) {
          expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), getColumnNumber(valueConstraint.get(0)));
        } else if(conditionType == 2) {
          expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), getColumnNumber(valueConstraint.get(0)));
        }
        String value = valueConstraint.get(2);
        if (value.matches("\\d*\\.\\d*")) {
          expr.type2 = new AttrType(AttrType.attrReal);
          expr.operand2.real = Float.valueOf(value);
        } else if (value.matches("\\d+")) {
          expr.type2 = new AttrType(AttrType.attrInteger);
          expr.operand2.integer = Integer.valueOf(value);
        } else {
          expr.type2 = new AttrType(AttrType.attrString);
          expr.operand2.string = value;
        }
      }


      if(itr.hasNext()){
        expr.next = new CondExpr();
      }
      expr = expr.next;
    }
    return expr_pointer;
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

// commenting as it is used in delete
//  public static List<RID> getRIDListHeapFile(List<String> valueConstraint, String columnFileName){
//
//    List<RID> ridList = new ArrayList<>();
//    try {
//
//      int colnum = Util.getColumnNumber(valueConstraint.get(0));
//      String filename = columnFileName + '.' + String.valueOf(colnum);
//      Columnarfile columnarFile = new Columnarfile(columnFileName);
//      AttrType[] types = columnarFile.getType();
//
//      AttrType[] attrs = new AttrType[1];
//      attrs[0] = types[colnum-1];
//
//      FldSpec[] projlist = new FldSpec[1];
//      RelSpec rel = new RelSpec(RelSpec.outer);
//      projlist[0] = new FldSpec(rel, 1);
//
//      short[] strsizes = new short[2];
//      strsizes[0] = 100;
//      strsizes[1] = 100;
//
//      CondExpr[] expr = Util.getValueContraint(valueConstraint);
//
//      ColumnarFileScan columnarFileScan = new ColumnarFileScan(columnFileName, filename, attrs, strsizes, (short) 1, 1, null, projlist, expr, false);
//      RID rid = new RID();
//      while(rid != null){
//        rid = columnarFileScan.get_next_rid();
//        ridList.add(rid);
//      }
//      columnarFileScan.close();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//
//    return ridList;
//
//  }

}
