package iterator;

public class RelSpec {



  public RelSpec(RelSpec that)
  {
    this.key = that.key;
  }

  public int key;

  public RelSpec() {
  }

  /**
   * constructor
   *
   * @param value teh enum value
   */
  public RelSpec(int value) {
    key = value;
  }

  public final static int outer = 0;
  public final static int innerRel = 1;
}  

