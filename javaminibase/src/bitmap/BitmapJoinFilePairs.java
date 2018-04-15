package bitmap;

import java.util.List;

public class BitmapJoinFilePairs {

  List<BitmapPair> filePairsList;

  BitmapJoinFilePairs next;

  public List<BitmapPair> getFilePairsList() {
    return filePairsList;
  }

  public void setFilePairsList(List<BitmapPair> filePairsList) {
    this.filePairsList = filePairsList;
  }

  public BitmapJoinFilePairs getNext() {
    return next;
  }

  public void setNext(BitmapJoinFilePairs next) {
    this.next = next;
  }
}
