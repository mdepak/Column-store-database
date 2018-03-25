package bitmap;

import diskmgr.Page;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import java.util.BitSet;

import java.io.IOException;

public class BMPage extends Page
        implements GlobalConst {

    public static final int DPFIXED = 2 * 2 + 3 * 4;
    public static final int MAX_RECORDS = (MAX_SPACE - DPFIXED)*8;

    public static final int RECORD_CNT = 0;
    //public static final int USED_BITS_PTR = 2;
    public static final int FREE_BITS = 2;
    //public static final int TYPE = 6;
    public static final int PREV_PAGE = 4;
    public static final int NEXT_PAGE = 8;
    public static final int CUR_PAGE = 12;

    /* Warning:
     These items must all pack tight, (no padding) for
     the current implementation to work properly.
     Be careful when modifying this class.
  */

    /**
     * number of bits in use
     */
    private short recordCnt;

    /**
     * offset of first used byte by data records in data[]
     */
    //private short usedBitsPtr;

    /**
     * number of bits free in data[]
     */
    private short freeBits;

    /**
     * an arbitrary value used by subclasses as needed
     */
    //private short type;

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
        //usedBitsPtr = Convert.getShortValue(USED_BITS_PTR, data);
        freeBits = Convert.getShortValue(FREE_BITS, data);
        recordCnt = Convert.getShortValue(RECORD_CNT, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        //System.out.println("usedBitsPtr= " + usedBitsPtr);
        System.out.println("freeBits= " + freeBits);
        System.out.println("recordCnt= " + recordCnt);

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

        recordCnt = 0;                // no slots in use
        Convert.setShortValue(recordCnt, RECORD_CNT, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

        //usedBitsPtr = (short) MAX_SPACE*8;  // offset in data array (grow backwards)
        //Convert.setShortValue(usedBitsPtr, USED_BITS_PTR, data);

        freeBits = (short) (MAX_SPACE*8 - DPFIXED*8);    // amount of space available
        Convert.setShortValue(freeBits, FREE_BITS, data);

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
     * @return RecordCnt used in this page
     * @throws IOException I/O errors
     */
    public short getRecordCnt()
            throws IOException {
        recordCnt = Convert.getShortValue(RECORD_CNT, data);
        return recordCnt;
    }

    /**
     * set bit at position
     *
     * @param position position to be set
     * @return
     * @throws IOException I/O errors in C++ Status insertRecord(char *recPtr, int recLen, RID& rid)
     */
    public boolean insertRecord(int position,boolean valueMatch, boolean updateFlag)
            throws IOException {
        //Need to implement this
        freeBits = Convert.getShortValue(FREE_BITS, data);
        if (freeBits < 1) {
            return false;

        } else {
            int pos = DPFIXED + position;
            BitSet bs = BitSet.valueOf(data);
            bs.set(pos,valueMatch);
            data = bs.toByteArray();

            freeBits -= 1;
            Convert.setShortValue(freeBits, FREE_BITS, data);
            if(updateFlag == false){ //inserting new record
                recordCnt = Convert.getShortValue(RECORD_CNT, data);
                recordCnt++;
                Convert.setShortValue(recordCnt, RECORD_CNT, data);
            }

            //usedBitsPtr = Convert.getShortValue(USED_BITS_PTR, data);
            //usedBitsPtr -= 1;    // adjust usedBitsPtr
            //Convert.setShortValue(usedBitsPtr, USED_BITS_PTR, data);
            return true;
        }


    }
    /**
     * reset bit at position
     *
     * @param position position to be reset
     * @return
     * @throws IOException I/O errors in C++ Status insertRecord(char *recPtr, int recLen, RID& rid)
     */
    public boolean deleteRecord(int position)
            throws IOException {
        //Need to implement this
        recordCnt = Convert.getShortValue(RECORD_CNT, data);
        if(position > recordCnt){
            return false;
            //position given is more than the number of re
        }
        else{
            recordCnt = Convert.getShortValue(RECORD_CNT, data);
            int pos = DPFIXED + position;

            BitSet bs = BitSet.valueOf(data);
            bs.set(pos, false);
            data = bs.toByteArray();

            freeBits -= 1;
            Convert.setShortValue(freeBits, FREE_BITS, data);

            recordCnt--;
            Convert.setShortValue(recordCnt, RECORD_CNT, data);

            //usedBitsPtr = Convert.getShortValue(USED_BITS_PTR, data);
            //usedBitsPtr -= 1;    // adjust usedBitsPtr
            //Convert.setShortValue(usedBitsPtr, USED_BITS_PTR, data);
            return true;
        }
    }

    void writeBMPageArray(byte[] data) {
        // Need to implement
        System.arraycopy(data, 0, this.data, DPFIXED, MAX_SPACE-DPFIXED);
    }
    /**
     * @return byte array
     */
    byte[] getBMPageArray(){
        // needed to reverse bits string using a for loop for returning.
        byte[] bitdata = new byte[MAX_SPACE-DPFIXED];
        System.arraycopy(this.data, DPFIXED, bitdata, 0, MAX_SPACE-DPFIXED);
        return bitdata;
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
        freeBits = Convert.getShortValue(FREE_BITS, data);
        return (freeBits);
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
        recordCnt = Convert.getShortValue(RECORD_CNT, data);
        if(recordCnt==0){
            return true;
        }
        else{
            return false;
        }
    }

    public void incrementRecordCount() throws IOException {
        recordCnt = Convert.getShortValue(RECORD_CNT, data);
        recordCnt++;
        Convert.setShortValue(recordCnt, RECORD_CNT, data);
    }

}
