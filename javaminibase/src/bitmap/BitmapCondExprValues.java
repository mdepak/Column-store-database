package bitmap;

import java.util.List;

public class BitmapCondExprValues {

  private List<Boolean> values;
  private BitmapCondExprValues next;

  public BitmapCondExprValues() {
  }

  public List<Boolean> getValues() {
    return values;
  }

  public void setValues(List<Boolean> values) {
    this.values = values;
  }

  public BitmapCondExprValues getNext() {
    return next;
  }

  public void setNext(BitmapCondExprValues next) {
    this.next = next;
  }

  public BitmapCondExprValues(List<Boolean> values) {
    this.values = values;
  }
}
