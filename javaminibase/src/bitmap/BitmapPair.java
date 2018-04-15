package bitmap;

public class BitmapPair {
  private String leftBitmapFile;
  private String rightBitmapFile;

  public String getLeftBitmapFile() {
    return leftBitmapFile;
  }

  public void setLeftBitmapFile(String leftBitmapFile) {
    this.leftBitmapFile = leftBitmapFile;
  }

  public String getRightBitmapFile() {
    return rightBitmapFile;
  }

  public void setRightBitmapFile(String rightBitmapFile) {
    this.rightBitmapFile = rightBitmapFile;
  }

  public BitmapPair(String leftBitmapFile, String rightBitmapFile) {
    this.leftBitmapFile = leftBitmapFile;
    this.rightBitmapFile = rightBitmapFile;
  }
}
