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
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.DataPageInfo;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import java.io.IOException;
import java.util.Arrays;


public class BitMapFile {

  private PageId headerPageId;
  public PageId _firstDirPageId;   // page number of header page
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
    _firstDirPageId = headerPage.getFirstDirPage();
  }

  public BitMapFile(String filename, Columnarfile columnfile,
      int columnNo, ValueClass value)
      throws IOException, HFDiskMgrException, HFBufMgrException, HFException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException, ConstructPageException, GetFileEntryException, FieldNumberOutOfBoundException, BMBufMgrException, BMException, PinPageException, UnpinPageException, AddFileEntryException {
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

      //add the bitmap header page to the file entry
      add_file_entry(filename, headerPageId);

      //Create and init the first directory page
      HFPage firstDirPage = new HFPage();

      Page apage = new Page();
      _firstDirPageId = newPage(apage, 1);

      firstDirPage.init(_firstDirPageId, apage);
      PageId pageId = new PageId(INVALID_PAGE);
      firstDirPage.setNextPage(pageId);
      firstDirPage.setPrevPage(pageId);


      unpinPage(_firstDirPageId, true /*dirty*/);

      headerPage.setFirstDirPage(_firstDirPageId);
      createBitMapIndex();
    } else {
      headerPage = new BitMapHeaderPage(headerPageId);
    }
  }

  private void createBitMapIndex()
      throws java.io.IOException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException, FieldNumberOutOfBoundException, BMBufMgrException, BMException, PinPageException, UnpinPageException {

    Scan scan = columnfile.openColumnScan(columnNo);// columnarFileScan instead of scan

    RID rid = new RID();

    Page pageinbuffer = new Page();

    HFPage currentDirPage = new HFPage();
    PageId currentDirPageId = new PageId(_firstDirPageId.pid);

    pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

    DataPageInfo dpinfo = new DataPageInfo();
    BMPage currentDataPage = newDatapage(dpinfo);
    PageId nextDirPageId = new PageId();  // OK
    HFPage nextDirPage = new HFPage();

    Tuple atuple = dpinfo.convertToTuple();
    byte[] tmpData = atuple.getTupleByteArray();
    RID firstRID = currentDirPage.insertRecord(tmpData);

    //Set the pointers appropriately.
    currentDataPage.setPrevPage(new PageId(Page.INVALID_PAGE));
    currentDataPage.setNextPage(new PageId(Page.INVALID_PAGE));

    ValueClass valueClass = Util.valueClassFactory(columnfile.getType()[columnNo - 1]);

    Tuple temp = null;

    try {
      temp = scan.getNext(rid);
    } catch (RuntimeException e) {
      throw e;
    }
    int position = 0;
    while (temp != null) {
      // Copy to another variable so that the fields of the tuple are initialized.

      Tuple tuple = new Tuple(temp.getTupleByteArray());
      tuple.tupleCopy(temp);
      valueClass.setValueFromColumnTuple(tuple, 1);

      if(currentDataPage.available_space()<1){
        //When the page is full - unpin the page

        // When the current BM Page cannot hold the data create new BM page

        //Check whether the current Directory has the space to hold the datapageinfo
        if(currentDirPage.available_space() >= dpinfo.size)
        {
          BMPage nextBMPage = newDatapage(dpinfo);
          nextBMPage.setPrevPage(currentDataPage.getCurPage());
          nextBMPage.setNextPage(new PageId(Page.INVALID_PAGE));
          currentDataPage.setNextPage(nextBMPage.getCurPage());
          position = 0;
          //TODO: Check for the proper place to unpin page
          unpinPage(currentDataPage.getCurPage(), true);

          currentDataPage = nextBMPage;
          atuple = dpinfo.convertToTuple();
          tmpData = atuple.getTupleByteArray();
          currentDirPage.insertRecord(tmpData);

        }
        else
        {
          nextDirPageId = currentDirPage.getNextPage();
          if (nextDirPageId.pid != INVALID_PAGE) { //Start IF03
            unpinPage(currentDirPageId, false);

            currentDirPageId.pid = nextDirPageId.pid;

            pinPage(currentDirPageId,
                currentDirPage, false);

            // now go back to the beginning of the outer while-loop and
            // search on the current directory page for a suitable datapage
          } //End of IF03
          else {  //Start Else03
            // case (2.2): append a new directory page after currentDirPage
            //             since it is the last directory page
            nextDirPageId = newPage(pageinbuffer, 1);
            // need check error!
            if (nextDirPageId == null) {
              throw new HFException(null, "can't new pae");
            }

            // initialize new directory page
            nextDirPage.init(nextDirPageId, pageinbuffer);
            PageId temppid = new PageId(INVALID_PAGE);
            nextDirPage.setNextPage(temppid);
            nextDirPage.setPrevPage(currentDirPageId);

            // update current directory page and unpin it
            // currentDirPage is already locked in the Exclusive mode
            currentDirPage.setNextPage(nextDirPageId);
            unpinPage(currentDirPageId, true/*dirty*/);

            currentDirPageId.pid = nextDirPageId.pid;
            currentDirPage = new HFPage(nextDirPage);

            // remark that MINIBASE_BM->newPage already
            // pinned the new directory page!
            // Now back to the beginning of the while-loop, using the
            // newly created directory page.

          }
        }
      }

      if (valueClass.equals(value)) {
        currentDataPage.insertRecord(position,true, false);
      }
      else{
        currentDataPage.insertRecord(position,false, false);
      }
      // Unpin page

      //unpinPage(currentDataPage.getCurPage(), true);

      temp = scan.getNext(rid);

      position+=1;
      int hdr_recordCont = headerPage.getRecordCount();
      hdr_recordCont+=1;
      headerPage.setRecordCount(hdr_recordCont);
    }

    unpinPage(currentDataPage.getCurPage(), true);
    unpinPage(currentDirPage.getCurPage(), true);

    //TODO: Check if this is required.
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


  private void add_file_entry(String fileName, PageId pageno)
      throws AddFileEntryException {
    try {
      SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
    } catch (Exception e) {
      e.printStackTrace();
      throw new AddFileEntryException(e, "");
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



  void close()
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
    if (headerPage != null) {
      SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
      headerPage = null;
    }
  }

  public void destroyBitMapFile() {

  }

  public void performQuery(String _columnfile, int[] selectedCols)
      throws HFBufMgrException, BMException, IOException, InvalidTupleSizeException, FieldNumberOutOfBoundException, InvalidSlotNumberException, HFDiskMgrException, HFException, InvalidTypeException {
    PageId currentDirPageId = new PageId(_firstDirPageId.pid);

    HFPage currentDirPage = new HFPage();
    BMPage currentDataPage = new BMPage();
    RID currentDataPageRid = new RID();
    PageId nextDirPageId = new PageId();

    //Scan scan = new Scan(file);
    RID rid = new RID();

    Tuple temp = null;

    //temp = scan.getNext(rid);


    //Main ROW POSITION
    int rowPosition = 0;

    pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

    Tuple atuple = new Tuple();

    Columnarfile cf = new Columnarfile(_columnfile);
    AttrType[] attrType = cf.getType();
    int numOfOutputColumns = selectedCols.length;
    AttrType[] reqAttrType = new AttrType[numOfOutputColumns];

    short[] str_sizes = new short[numOfOutputColumns];

    //TODO: Check this is proper str sizes
    short[] s_sizes = cf.getStrSizes();
    int j = 0;
    for(int i=0; i<numOfOutputColumns; i++) {
      reqAttrType[i] = attrType[selectedCols[i] - 1];
      if(reqAttrType[i].attrType == AttrType.attrString) {
        str_sizes[j] = s_sizes[selectedCols[i] - 1];
        j++;
      }
    }

    short[] strSizes = Arrays.copyOfRange(str_sizes, 0, j);

    while (currentDirPageId.pid != INVALID_PAGE) {// Start While01

      for (currentDataPageRid = currentDirPage.firstRecord();
          currentDataPageRid != null;
          currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid)) {
        atuple = currentDirPage.getRecord(currentDataPageRid);
        DataPageInfo dpinfo = new DataPageInfo(atuple);
        try {
          pinPage(dpinfo.getPageId(), currentDataPage, false/*Rddisk*/);

          byte[] bitMapData = currentDataPage.getBMPageArray();

          int recordCount = currentDataPage.getRecordCnt();
          for(int index = 0; index<bitMapData.length && recordCount>0; index++)
          {
            for(int position = 0; position<8 && recordCount>0; position++)
            {
              //Tuple tuple = new Tuple(temp.getTupleByteArray());
              //tuple.tupleCopy(temp);
              //ValueClass colVal = Util.valueClassFactory(columnfile.getType()[columnNo - 1]);
              //colVal.setValueFromColumnTuple(tuple, 1);
              String bitVal = Util.getBitAsString(bitMapData[index], position);

              if(bitVal.equalsIgnoreCase("1") )
              {
                //Postion of the row matching the condition
                //System.out.println("Position of the record matching condition");

                //Actual fetching of data
                Tuple tuple = new Tuple();
                tuple.setHdr((short) numOfOutputColumns, reqAttrType, strSizes);

                for(int i=0; i<numOfOutputColumns; i++){
                  int indexNumber = selectedCols[i];
                  Heapfile heapfile = cf.getColumnFiles()[indexNumber-1];
                  Tuple tupleTemp = Util.getTupleFromPosition(rowPosition, heapfile);
                  tupleTemp.initHeaders();

                  if(attrType[indexNumber-1].attrType == AttrType.attrString) {
                    tuple.setStrFld(i+1, tupleTemp.getStrFld(1));
                  }else if(attrType[indexNumber-1].attrType == AttrType.attrInteger) {
                    tuple.setIntFld(i+1, tupleTemp.getIntFld(1));
                  }else if(attrType[indexNumber-1].attrType == AttrType.attrReal) {
                    tuple.setFloFld(i+1, tupleTemp.getFloFld(1));
                  }
                }

                if(tuple != null) {
                  tuple.initHeaders();
                  for (int i = 0; i < tuple.noOfFlds(); i++) {
                    if (attrType[selectedCols[i] - 1].attrType == AttrType.attrString) {
                      System.out.println(tuple.getStrFld(i + 1));
                    }
                    if (attrType[selectedCols[i] - 1].attrType == AttrType.attrInteger) {
                      System.out.println(tuple.getIntFld(i + 1));
                    }
                    if (attrType[selectedCols[i] - 1].attrType == AttrType.attrReal) {
                      System.out.println(tuple.getFloFld(i + 1));
                    }
                  }
                  System.out.println("");
                }
              }
              rowPosition++;
              recordCount--;
              //temp = scan.getNext(rid);
            }
          }
          //Util.printBitsInByte(bitMapData);

          unpinPage(currentDataPage.getCurPage(), false/*undirty*/);
        } catch (Exception e) {
          throw e;
        }
      }

      nextDirPageId = currentDirPage.getNextPage();
      try {
        unpinPage(currentDirPageId, false /*undirty*/);
      } catch (Exception e) {
        throw new BMException(e, "heapfile,_find,unpinpage failed");
      }

      currentDirPageId.pid = nextDirPageId.pid;
      if (currentDirPageId.pid != INVALID_PAGE) {
        pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
        if (currentDirPage == null) {
          throw new BMException(null, "pinPage return null page");
        }
      }
    } // end of While01
    // checked all dir pages and all data pages; user record not found:(
  }

  public void printBitMap(Heapfile file)
      throws IOException, PinPageException, UnpinPageException, BMException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, FieldNumberOutOfBoundException {
    PageId currentDirPageId = new PageId(_firstDirPageId.pid);

    HFPage currentDirPage = new HFPage();
    BMPage currentDataPage = new BMPage();
    RID currentDataPageRid = new RID();
    PageId nextDirPageId = new PageId();

    Scan scan = new Scan(file);
    RID rid = new RID();

    Tuple temp = null;

    temp = scan.getNext(rid);

    pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

    Tuple atuple = new Tuple();

    while (currentDirPageId.pid != INVALID_PAGE) {// Start While01

      for (currentDataPageRid = currentDirPage.firstRecord();
          currentDataPageRid != null;
          currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid)) {
        atuple = currentDirPage.getRecord(currentDataPageRid);
        DataPageInfo dpinfo = new DataPageInfo(atuple);
        try {
          pinPage(dpinfo.getPageId(), currentDataPage, false/*Rddisk*/);

          byte[] bitMapData = currentDataPage.getBMPageArray();

          int recordCount = currentDataPage.getRecordCnt();
          for(int index = 0; index<bitMapData.length && recordCount>0; index++)
          {
              for(int position = 0; position<8 && recordCount>0; position++)
              {
                Tuple tuple = new Tuple(temp.getTupleByteArray());
                tuple.tupleCopy(temp);
                ValueClass colVal = Util.valueClassFactory(columnfile.getType()[columnNo - 1]);
                colVal.setValueFromColumnTuple(tuple, 1);
                String bitVal = Util.getBitAsString(bitMapData[index], position);
                if(colVal.equals(value) && bitVal.equalsIgnoreCase("1") || (!colVal.equals(value) && bitVal.equalsIgnoreCase("0")))
                {
                  System.out.println("Proper bit set");
                }
                else
                {
                  System.out.println("Improper bit set");
                }
                recordCount--;
                temp = scan.getNext(rid);
              }
          }
          //Util.printBitsInByte(bitMapData);

          unpinPage(currentDataPage.getCurPage(), false/*undirty*/);
        } catch (Exception e) {
          throw e;
        }
      }

      nextDirPageId = currentDirPage.getNextPage();
      try {
        unpinPage(currentDirPageId, false /*undirty*/);
      } catch (Exception e) {
        throw new BMException(e, "heapfile,_find,unpinpage failed");
      }

      currentDirPageId.pid = nextDirPageId.pid;
      if (currentDirPageId.pid != INVALID_PAGE) {
        pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
        if (currentDirPage == null) {
          throw new BMException(null, "pinPage return null page");
        }
      }
    } // end of While01
    // checked all dir pages and all data pages; user record not found:(
  }

  BitMapHeaderPage getHeaderPage() {
    return headerPage;
  }

  boolean delete(int position)
      throws PinPageException, IOException, InvalidTupleSizeException, UnpinPageException, BMException, InvalidSlotNumberException, HFBufMgrException {

    int deletePosition = position;

    PageId currentDirPageId = new PageId(_firstDirPageId.pid);

    HFPage currentDirPage = new HFPage();
    BMPage currentDataPage = new BMPage();
    RID currentDataPageRid = new RID();
    PageId nextDirPageId = new PageId();
    // datapageId is stored in dpinfo.pageId

    pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

    Tuple atuple = new Tuple();

    while (currentDirPageId.pid != INVALID_PAGE) {// Start While01
      // ASSERTIONS:
      //  currentDirPage, currentDirPageId valid and pinned and Locked.

      for (currentDataPageRid = currentDirPage.firstRecord();
          currentDataPageRid != null;
          currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid)) {
        atuple = currentDirPage.getRecord(currentDataPageRid);
        DataPageInfo dpinfo = new DataPageInfo(atuple);

        // ASSERTIONS:
        // - currentDataPage, currentDataPageRid, dpinfo valid
        // - currentDataPage pinned

        if (deletePosition < dpinfo.recct) {
          //Perform actual delete code
          //Pin the BMPage and perform the actual operation

          try {
            pinPage(dpinfo.getPageId(), currentDataPage, false/*Rddisk*/);
            currentDataPage.deleteRecord(deletePosition);
            unpinPage(currentDirPageId, true/*undirty*/);
            //check error;need unpin currentDirPage
          } catch (Exception e) {
            throw e;
          }

        } else {
          deletePosition -= dpinfo.recct;
        }
      }

      // if we would have found the correct datapage on the current
      // directory page we would have already returned.
      // therefore:
      // read in next directory page:

      nextDirPageId = currentDirPage.getNextPage();
      try {
        unpinPage(currentDirPageId, false /*undirty*/);
      } catch (Exception e) {
        throw new BMException(e, "heapfile,_find,unpinpage failed");
      }

      currentDirPageId.pid = nextDirPageId.pid;
      if (currentDirPageId.pid != INVALID_PAGE) {
        pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
        if (currentDirPage == null) {
          throw new BMException(null, "pinPage return null page");
        }
      }


    } // end of While01
    // checked all dir pages and all data pages; user record not found:(

    return false;
  }

  public boolean insert(int position)
      throws PinPageException, IOException, InvalidSlotNumberException, InvalidTupleSizeException, BMBufMgrException, HFException, HFBufMgrException, HFDiskMgrException, BMException, UnpinPageException, SpaceNotAvailableException {

    HFPage firstDirPage = new HFPage();

    pinPage(_firstDirPageId, firstDirPage, false);
    boolean found;
    RID currentDataPageRid = new RID();
    Page pageinbuffer = new Page();
    HFPage currentDirPage = new HFPage();
    BMPage currentDataPage = new BMPage();

    HFPage nextDirPage = new HFPage();
    PageId currentDirPageId = new PageId(_firstDirPageId.pid);
    PageId nextDirPageId = new PageId();  // OK

    pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);

    found = false;
    Tuple atuple;
    DataPageInfo dpinfo = new DataPageInfo();

    int recordPosition = position;

    while (recordPosition != 0) {
      for (currentDataPageRid = currentDirPage.firstRecord();
          currentDataPageRid != null;
          currentDataPageRid =
              currentDirPage.nextRecord(currentDataPageRid)) {
        atuple = currentDirPage.getRecord(currentDataPageRid);

        dpinfo = new DataPageInfo(atuple);

        if (dpinfo.getAvailspace() > 0) {
          found = true;
          break;
        }
      }

        if (!found) {
          // add a new page entry and make it point to the new DMPage created.
          if (currentDirPage.available_space() > 0) {
            currentDataPage = newDatapage(dpinfo);
            // currentDataPage is pinned! and dpinfo->pageId is also locked
            // in the exclusive mode

            // didn't check if currentDataPage==NULL, auto exception

            atuple = dpinfo.convertToTuple();

            byte[] tmpData = atuple.getTupleByteArray();
            currentDataPageRid = currentDirPage.insertRecord(tmpData);

            RID tmprid = currentDirPage.firstRecord();

            // need catch error here!
            if (currentDataPageRid == null) {
              throw new BMException(null, "no space to insert rec.");
            }

            // end the loop, because a new datapage with its record
            // in the current directorypage was created and inserted into
            // the heapfile; the new datapage has enough space for the
            // record which the user wants to insert
            found = true;
          }
          // Current directory page does not hold any
          else {  //Start else 02
            // case (2.2)
            nextDirPageId = currentDirPage.getNextPage();
            // two sub-cases:
            //
            // (2.2.1) nextDirPageId != INVALID_PAGE:
            //         get the next directory page from the buffer manager
            //         and do another look
            // (2.2.2) nextDirPageId == INVALID_PAGE:
            //         append a new directory page at the end of the current
            //         page and then do another loop

            if (nextDirPageId.pid != INVALID_PAGE) { //Start IF03
              // case (2.2.1): there is another directory page:
              unpinPage(currentDirPageId, false);

              currentDirPageId.pid = nextDirPageId.pid;

              pinPage(currentDirPageId,
                  currentDirPage, false);

              // now go back to the beginning of the outer while-loop and
              // search on the current directory page for a suitable datapage
            } //End of IF03
            else {  //Start Else03
              // case (2.2): append a new directory page after currentDirPage
              //             since it is the last directory page
              nextDirPageId = newPage(pageinbuffer, 1);
              // need check error!
              if (nextDirPageId == null) {
                throw new BMException(null, "can't new pae");
              }

              // initialize new directory page
              nextDirPage.init(nextDirPageId, pageinbuffer);
              PageId temppid = new PageId(INVALID_PAGE);
              nextDirPage.setNextPage(temppid);
              nextDirPage.setPrevPage(currentDirPageId);

              // update current directory page and unpin it
              // currentDirPage is already locked in the Exclusive mode
              currentDirPage.setNextPage(nextDirPageId);
              unpinPage(currentDirPageId, true/*dirty*/);

              currentDirPageId.pid = nextDirPageId.pid;
              currentDirPage = new HFPage(nextDirPage);

              // remark that MINIBASE_BM->newPage already
              // pinned the new directory page!
              // Now back to the beginning of the while-loop, using the
              // newly created directory page.

            } //End of else03
          }
        } else {
          // found == true:
          // we have found a datapage with enough space,
          // but we have not yet pinned the datapage:

          pinPage(dpinfo.getPageId(), currentDataPage, false);
        }
      }

    //Now the proper DM page is found and the page is pinned in the memory
    if ((dpinfo.getPageId()).pid == INVALID_PAGE) // check error!
    {
      throw new BMException(null, "invalid PageId");
    }

    if (!(currentDataPage.available_space() >= 0)) {
      throw new SpaceNotAvailableException(null, "no available space");
    }

    if (currentDataPage == null) {
      throw new BMException(null, "can't find Data page");
    }

    currentDataPage.insertRecord(position, true, true);

    dpinfo.recct++;
    dpinfo.availspace = currentDataPage.available_space();

    unpinPage(dpinfo.getPageId(), true /* = DIRTY */);

    // DataPage is now released
    atuple = currentDirPage.returnRecord(currentDataPageRid);
    DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);

    dpinfo_ondirpage.availspace = dpinfo.availspace;
    dpinfo_ondirpage.recct = dpinfo.recct;
    dpinfo_ondirpage.getPageId().pid = dpinfo.getPageId().pid;
    dpinfo_ondirpage.flushToTuple();

    unpinPage(currentDirPageId, true /* = DIRTY */);

    return true;
  }


  private BMPage newDatapage(DataPageInfo dpinfop)
      throws HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException, BMBufMgrException {
    Page apage = new Page();
    PageId pageId = new PageId();
    pageId = newPage(apage, 1);

    if (pageId == null) {
      throw new HFException(null, "can't new pae");
    }

    // initialize internal values of the new page:

    BMPage bmPage = new BMPage();
    bmPage.init(pageId, apage);

    dpinfop.setPageId(pageId);
    dpinfop.recct = 0;
    dpinfop.availspace = bmPage.available_space();

    return bmPage;
  }


  /**
   * short cut to access the pinPage function in bufmgr package.
   *
   * @see bufmgr.pinPage
   */
  public void pinPage(PageId pageno, Page page, boolean emptyPage)
      throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   *
   * @see bufmgr.unpinPage
   */
  public void unpinPage(PageId pageno, boolean dirty)
      throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    } catch (Exception e) {
      throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
    }

  } // end of unpinPage

}

