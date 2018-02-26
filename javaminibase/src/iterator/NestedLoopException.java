package iterator;

import chainexception.*;

public class NestedLoopException extends ChainException {

  public NestedLoopException(String s) {
    super(null, s);
  }

  public NestedLoopException(Exception prev, String s) {
    super(prev, s);
  }
}
