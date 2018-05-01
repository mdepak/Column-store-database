package iterator;

public class FldSpec {

  public RelSpec relation = null;
  public int offset;

  public FldSpec() {
  }



  public FldSpec(FldSpec that)
  {
    if (that != null){
      this.relation = new RelSpec(that.relation);
      this.offset = that.offset;
    }
  }


  /**
   * contrctor
   *
   * @param _relation the relation is outer or inner
   * @param _offset the offset of the field
   */
  public FldSpec(RelSpec _relation, int _offset) {
    relation = _relation;
    offset = _offset;
  }
}

