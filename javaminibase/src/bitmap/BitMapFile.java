package bitmap;

import static global.GlobalConst.INVALID_PAGE;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.DeleteFileEntryException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.Columnarfile;
import columnar.Util;
import columnar.ValueClass;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import java.io.IOException;


public class BitMapFile {

  private PageId headerPageId;
  public Columnarfile columnfile;
  public int columnNo;
  public ValueClass value;
  public String fileName;
  private BitMapHeaderPage headerPage;
  private String dbname;
  private Heapfile bitmapFile;

  public BitMapFile(String filename)
      throws IOException, HFException, HFBufMgrException, HFDiskMgrException, ConstructPageException, GetFileEntryException {
    headerPageId = get_file_entry(filename);
    headerPage = new BitMapHeaderPage(headerPageId);
    dbname = new String(filename);
  }

  public BitMapFile(String filename, Columnarfile columnfile,
      int columnNo, ValueClass value)
      throws IOException, HFDiskMgrException, HFBufMgrException, HFException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException, ConstructPageException, GetFileEntryException, FieldNumberOutOfBoundException, BMBufMgrException, BMException, PinPageException, UnpinPageException {
    headerPageId = get_file_entry(filename);
    this.columnNo = columnNo;
    this.columnfile = columnfile;
    this.value = value;

    // Bit Map file already does not exist with this name
    if (headerPageId == null) {
      headerPage = new BitMapHeaderPage();
      headerPageId = headerPage.getPageId();
      headerPage.setColumnNo(columnNo);
      headerPage.setPrevPage(new PageId(INVALID_PAGE));
      headerPage.setNextPage(new PageId(INVALID_PAGE));
      createBitMapIndex();
    } else {
      headerPage = new BitMapHeaderPage(headerPageId);
    }
  }

  private void createBitMapIndex()
      throws java.io.IOException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException, FieldNumberOutOfBoundException, BMBufMgrException, BMException, PinPageException, UnpinPageException {

    Scan scan = columnfile.openColumnScan(columnNo);// columnarFileScan instead of scan

    RID rid = new RID();
    PageId firstBMPage = headerPage.getFirstBMPage();
    BMPage currPage = new BMPage();
    pinPage(firstBMPage, currPage, false);

    ValueClass valueClass = Util.valueClassFactory(columnfile.getType()[columnNo - 1]);

    Tuple temp = null;

    try {
      temp = scan.getNext(rid);
    } catch (RuntimeException e) {
     throw e;
    }

    while (temp != null) {
      // Copy to another variable so that the fields of the tuple are initialized.

      Tuple tuple = new Tuple(temp.getTupleByteArray());
      tuple.tupleCopy(temp);
      valueClass.setValueFromColumnTuple(tuple, 1);

      // If values match set the bit position as 1 otherwise it is '0' by default - don't need to handle it
      if (currPage.available_space() > 0) {
        //Increment the records
        currPage.recordCount++;
      } else {
        // Create a new BMPage and set the proper values
        BMPage nextPage = addNextBMPage(currPage);

        // Unpin page
        unpinPage(currPage.getCurPage(), true);

        currPage = nextPage;
        currPage.recordCount++;
      }
      if (valueClass.equals(value)) {
        currPage.insertRecord(headerPage.recordCount / BMPage.MAX_RECORDS);
      }

      temp = scan.getNext(rid);
      headerPage.recordCount++;
    }

    unpinPage(headerPage.getPageId(), true);
  }

  /**
   * Add a new BMPage next to the Current BMPage
   */
  private BMPage addNextBMPage(BMPage currPage) throws IOException, BMException, BMBufMgrException {
    BMPage newPage = getNewBMPage();
    currPage.setNextPage(newPage.getCurPage());
    newPage.setPrevPage(currPage.getCurPage());
    return newPage;
  }


  /* get a new BMpage from the buffer manager and initialize dpinfo
     @param dpinfop the information in the new HFPage
  */
  private BMPage getNewBMPage()
      throws BMException,
      IOException, BMBufMgrException {
    Page apage = new Page();
    PageId pageId = new PageId();
    pageId = newPage(apage, 1);

    if (pageId == null) {
      throw new BMException(null, "can't new pae");
    }

    // initialize internal values of the new page:
    BMPage bmPage = new BMPage();
    bmPage.init(pageId, apage);

    return bmPage;
  }


  private PageId newPage(Page page, int num)
      throws BMBufMgrException {

    PageId tmpId = new PageId();

    try {
      tmpId = SystemDefs.JavabaseBM.newPage(page, num);
    } catch (Exception e) {
      throw new BMBufMgrException(e, "Heapfile.java: newPage() failed");
    }

    return tmpId;

  } // end of newPage

  private PageId get_file_entry(String filename) throws GetFileEntryException {
    try {
      return SystemDefs.JavabaseDB.get_file_entry(filename);
    } catch (Exception e) {
      e.printStackTrace();
      throw new GetFileEntryException(e, "");
    }
  }

  private void pinPage(PageId pageno, Page page, boolean emptyPage)
      throws PinPageException {
    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
    } catch (Exception e) {
      e.printStackTrace();
      throw new PinPageException(e, "");
    }
  }

  private void add_file_entry(String fileName, PageId pageno)
      throws AddFileEntryException {
    try {
      SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
    } catch (Exception e) {
      e.printStackTrace();
      throw new AddFileEntryException(e, "");
    }
  }

  private void unpinPage(PageId pageno)
      throws UnpinPageException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
    } catch (Exception e) {
      e.printStackTrace();
      throw new UnpinPageException(e, "");
    }
  }

  private void freePage(PageId pageno)
      throws FreePageException {
    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    } catch (Exception e) {
      e.printStackTrace();
      throw new FreePageException(e, "");
    }

  }

  private void delete_file_entry(String filename)
      throws DeleteFileEntryException {
    try {
      SystemDefs.JavabaseDB.delete_file_entry(filename);
    } catch (Exception e) {
      e.printStackTrace();
      throw new DeleteFileEntryException(e, "");
    }
  }

  private void unpinPage(PageId pageno, boolean dirty)
      throws UnpinPageException {
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    } catch (Exception e) {
      e.printStackTrace();
      throw new UnpinPageException(e, "");
    }
  }


  void close()
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
    if (headerPage != null) {
      SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
      headerPage = null;
    }
  }

  public void destroyBitMapFile() {

  }


  public  void printBitMap(BitMapHeaderPage header)
      throws IOException, PinPageException, UnpinPageException {

    BMPage currPage = new BMPage();
    pinPage(header.getFirstBMPage(), currPage, false);

    while(true)
    {
      byte[] data = currPage.getBMPageArray();

      //Print the byte data as bit in every byte
      Util.printBitsInByte(data);

      if(currPage.getNextPage().pid!= Page.INVALID_PAGE)
      {
        break;
      }

      PageId nextPageId = currPage.getNextPage();
      unpinPage(currPage.getCurPage(), false);
      pinPage(nextPageId, currPage, false);
    }
  }

  BitMapHeaderPage getHeaderPage() {
    return headerPage;
  }

  boolean Delete(int position) {
    return false;
  }

  public boolean insert(int position) {
    return false;
  }
}

