package bitmap;

import btree.ConstructPageException;
import diskmgr.Page;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;
import java.io.IOException;

class BitMapHeaderPage extends HFPage {

  public int columnNo; // Slot 1
  public int recordCount; //Slot 2
  public PageId firstDirPage = new PageId(); //Slot 3
  //TODO: Add file name if necessary

  void setColumnNo(int columnNo) throws IOException {
    this.columnNo = columnNo;
    setSlot(1, columnNo, 0);
  }

  int getColumnNo() throws IOException {
    return getSlotLength(1);
  }

  void setFirstDirPage(PageId pageno) throws IOException {
    firstDirPage.pid = pageno.pid;
    setSlot(3, firstDirPage.pid, 0);
  }

  public PageId getFirstDirPage() throws IOException {
    firstDirPage.pid = getSlotLength(3);
    return firstDirPage;
  }

  void setPageId(PageId pageno) throws IOException {
    setCurPage(pageno);
  }

  PageId getPageId()
      throws IOException {
    return getCurPage();
  }


  public BitMapHeaderPage(PageId pageno) throws ConstructPageException {
    super();
    try {
      SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
    } catch (Exception e) {
      throw new ConstructPageException(e, "pinpage failed");
    }
  }

  /**
   * associate the SortedPage instance with the Page instance
   */
  public BitMapHeaderPage(Page page) {

    super(page);
  }


  public int getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(int recordCount) throws IOException {
    this.recordCount = recordCount;
    setSlot(2, recordCount, 0);
  }

  /**
   * new a page, and associate the SortedPage instance with the Page instance
   */

  public BitMapHeaderPage() throws ConstructPageException {
    super();
    try {
      Page apage = new Page();
      PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
      if (pageId == null) {
        throw new ConstructPageException(null, "new page failed");
      }
      this.init(pageId, apage);

    } catch (Exception e) {
      throw new ConstructPageException(e, "construct header page failed");
    }
  }
}