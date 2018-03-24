package bitmap;

import chainexception.ChainException;

public class BMException extends ChainException {


  public BMException() {
    super();

  }

  public BMException(Exception ex, String name) {
    super(ex, name);
  }

}
