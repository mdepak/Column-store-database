package bitmap;

import java.util.List;

public class BitmapCondExprScanFiles {
  List<BitMapFile> scanList;

  BitmapCondExprScanFiles next;

  public List<BitMapFile> getScanList() {
    return scanList;
  }

  public void setScanList(List<BitMapFile> scanList) {
    this.scanList = scanList;
  }

  public BitmapCondExprScanFiles getNext() {
    return next;
  }

  public void setNext(BitmapCondExprScanFiles next) {
    this.next = next;
  }
}
