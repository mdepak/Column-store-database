package bitmap;

import diskmgr.Page;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;

import java.io.IOException;

public class BMPage extends Page
        implements GlobalConst {

    public static final int DPFIXED = 4 * 2 + 3 * 4;

    public static final int SLOT_CNT = 0;
    public static final int USED_PTR = 2;
    public static final int FREE_SPACE = 4;
    //public static final int TYPE = 6;
    public static final int PREV_PAGE = 8;
    public static final int NEXT_PAGE = 12;
    public static final int CUR_PAGE = 16;

    /* Warning:
     These items must all pack tight, (no padding) for
     the current implementation to work properly.
     Be careful when modifying this class.
  */

    /**
     * number of slots in use
     */
    private short slotCnt;

    /**
     * offset of first used byte by data records in data[]
     */
    private short usedPtr;

    /**
     * number of bytes free in data[]
     */
    private short freeSpace;

    /**
     * an arbitrary value used by subclasses as needed
     */
    private short type;

    /**
     * backward pointer to data page
     */
    private PageId prevPage = new PageId();

    /**
     * forward pointer to data page
     */
    private PageId nextPage = new PageId();

    /**
     * page number of this page
     */
    protected PageId curPage = new PageId();

    /**
     * Default constructor
     */
    public BMPage(){
        //Default constructor
    }

    public BMPage(Page page) {
        //Constructor of class BMPage open a BMPage and make this BMpage point to the given page
        data = page.getpage();
    }

    /**
     * Dump contents of a page
     *
     * @throws IOException I/O errors
     */
    void dumpPage()
            throws IOException {
        //Dump contents of a page
        int i, n;
        int length, offset;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        usedPtr = Convert.getShortValue(USED_PTR, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("usedPtr= " + usedPtr);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("slotCnt= " + slotCnt);

//        for (i = 0, n = DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
//            length = Convert.getShortValue(n, data);
//            offset = Convert.getShortValue(n + 2, data);
//            System.out.println("slotNo " + i + " offset= " + offset);
//            System.out.println("slotNo " + i + " length= " + length);
//        }
    }

    void init(PageId pageNo, Page apage)
        //    Constructor of class BMPage initialize a new page
            throws IOException {
        data = apage.getpage();

        slotCnt = 0;                // no slots in use
        Convert.setShortValue(slotCnt, SLOT_CNT, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        usedPtr = (short) MAX_SPACE;  // offset in data array (grow backwards)
        Convert.setShortValue(usedPtr, USED_PTR, data);

        freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);

    }
    void openBMpage(Page apage){
        //    Constructor of class BMPage open a existed BMPage
        data = apage.getpage();
    }

    /**
     * @return page number of current page
     * @throws IOException I/O errors
     */
    public PageId getCurPage()
            throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     *
     * @param pageNo page number for current page
     * @throws IOException I/O errors
     */
    public void setCurPage(PageId pageNo)
            throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    /**
     * @return PageId of previous page
     * @throws IOException I/O errors
     */
    public PageId getPrevPage()
            throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }
    /**
     * sets value of prevPage to pageNo
     *
     * @param pageNo page number for previous page
     * @throws IOException I/O errors
     */
    public void setPrevPage(PageId pageNo)
            throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }

    /**
     * @return page number of next page
     * @throws IOException I/O errors
     */
    public PageId getNextPage()
            throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    /**
     * sets value of nextPage to pageNo
     *
     * @param pageNo page number for next page
     * @throws IOException I/O errors
     */
    public void setNextPage(PageId pageNo)
            throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    /**
     * @return slotCnt used in this page
     * @throws IOException I/O errors
     */
    public short getSlotCnt()
            throws IOException {
        slotCnt = Convert.getShortValue(SLOT_CNT, data);
        return slotCnt;
    }

    /**
     * sets slot contents
     *
     * @param slotno the slot number
     * @param length length of record the slot contains
     * @param offset offset of record
     * @throws IOException I/O errors
     */
    public void setSlot(int slotno, int length, int offset)
            throws IOException {
//        int position = DPFIXED + slotno * SIZE_OF_SLOT;
//        Convert.setShortValue((short) length, position, data);
//        Convert.setShortValue((short) offset, position + 2, data);
    }

    /**
     * set bit at position
     *
     * @param position position to be set
     * @return
     * @throws IOException I/O errors in C++ Status insertRecord(char *recPtr, int recLen, RID& rid)
     */
    public void insertRecord(int position)
            throws IOException {
        //Need to implement this
    }
    /**
     * reset bit at position
     *
     * @param position position to be reset
     * @return
     * @throws IOException I/O errors in C++ Status insertRecord(char *recPtr, int recLen, RID& rid)
     */
    public void deleteRecord(int position)
            throws IOException {
        //Need to implement this
    }
    void writeBMPageArray(byte[] data){
        // Need to implement
        // only bits not the metadata
    }

    /**
     * @return byte array
     */
    byte[] getBMPageArray(){
        // Need to implement
        // only bits not the metadata
        return data;
    }

    /**
     * returns the amount of available space on the page.
     *
     * @return the amount of available space on the page
     * @throws IOException I/O errors
     */
    public int available_space()
            throws IOException {
        //check if this is correct
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return (freeSpace - 1);
    }


    /**
     * Determining if the page is empty
     *
     * @return true if the HFPage is has no records in it, false otherwise
     * @throws IOException I/O errors
     */
    public boolean empty()
            throws IOException {
        //Need to implement

        return true;
    }

}
