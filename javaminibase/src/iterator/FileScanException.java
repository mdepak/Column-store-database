package iterator;

import chainexception.*;

public class FileScanException extends ChainException {

  public FileScanException(String s) {
    super(null, s);
  }

  public FileScanException(Exception prev, String s) {
    super(prev, s);
  }
}
