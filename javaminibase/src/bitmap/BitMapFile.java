package bitmap;

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
import columnar.FloatValue;
import columnar.IntegerValue;
import columnar.StringValue;
import columnar.ValueClass;
import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.RID;
import global.SystemDefs;
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
import java.util.Arrays;


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
      throws IOException, HFDiskMgrException, HFBufMgrException, HFException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException, ConstructPageException, GetFileEntryException {
    headerPageId = get_file_entry(filename);
    // Bit Map file already does not exist with this name
    if (headerPageId == null) {
      headerPage = new BitMapHeaderPage();
      headerPageId = headerPage.getPageId();
      headerPage.setColumnNo(columnNo);
      createBitMapIndex();
    } else {
      headerPage = new BitMapHeaderPage(headerPageId);
    }
  }

  private boolean createBitMapIndex()
      throws java.io.IOException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {
    Scan cfs = this.columnfile.openColumnScan(this.columnNo);// columnarFileScan instead of scan

    RID rid = new RID();
    Tuple tScan = new Tuple();

    return true;
  }

  private PageId get_file_entry(String filename) throws GetFileEntryException
  {
    try {
      return SystemDefs.JavabaseDB.get_file_entry(filename);
    } catch (Exception e) {
      e.printStackTrace();
      throw new GetFileEntryException(e, "");
    }
  }

  private Page pinPage(PageId pageno)
      throws PinPageException {
    try {
      Page page = new Page();
      SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
      return page;
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

  void createBitMapFile(BitMapHeaderPage headerPage, HFPage heapPage) {

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

