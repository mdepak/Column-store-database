package bitmap;

import chainexception.ChainException;

public class BMBufMgrException extends ChainException {


  public BMBufMgrException() {
    super();

  }

  public BMBufMgrException(Exception ex, String name) {
    super(ex, name);
  }


}
