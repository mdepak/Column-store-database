package bitmap;

import java.util.List;

public class BitmapCondExprScans {
    List<BitmapScan> scanList;

    BitmapCondExprScans next;

    public List<BitmapScan> getScanList() {
        return scanList;
    }

    public void setScanList(List<BitmapScan> scanList) {
        this.scanList = scanList;
    }

    public BitmapCondExprScans getNext() {
        return next;
    }

    public void setNext(BitmapCondExprScans next) {
        this.next = next;
    }
}
