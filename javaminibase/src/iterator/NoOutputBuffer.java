package iterator;

import chainexception.*;

public class NoOutputBuffer extends ChainException {

  public NoOutputBuffer(String s) {
    super(null, s);
  }

  public NoOutputBuffer(Exception prev, String s) {
    super(prev, s);
  }
}
