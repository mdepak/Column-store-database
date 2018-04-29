package bitmap;

import java.util.List;

public class BitmapJoinpairNestedLoopScans {

  List<BitmapNestedLoopScan> nestedLoopScanList;

  BitmapJoinpairNestedLoopScans next;


  public List<BitmapNestedLoopScan> getNestedLoopScanList() {
    return nestedLoopScanList;
  }

  public void setNestedLoopScanList(List<BitmapNestedLoopScan> nestedLoopScanList) {
    this.nestedLoopScanList = nestedLoopScanList;
  }

  public BitmapJoinpairNestedLoopScans getNext() {
    return next;
  }

  public void setNext(BitmapJoinpairNestedLoopScans next) {
    this.next = next;
  }
}
