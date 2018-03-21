package bitmap;

import diskmgr.Page;
import global.PageId;
import heap.HFPage;

public class BitMapPage extends HFPage {

    int keyType;

    public BitMapPage() {
        // TODO Auto-generated constructor stub
    }

    public BitMapPage(Page page, int keyType)
           // throws IOException, ConstructPageException
    {
        super(page);
        this.keyType = keyType;
    }

    public int   available_space()
    {
        return 0;
    }

    public void   dumpPage()
    {

    }

    public boolean empty()
    {
        return true;
    }

    public void   init(PageId pageNo, Page apage)
    {

    }

    void   openBMpage(Page apage)
    {

    }

    public PageId  getCurPage()
    {
        return new PageId(1);
    }

    public PageId  getNextPage()
    {
        return new PageId(1);
    }

    public PageId  getPrevPage()
    {
        return new PageId(1);
    }

    public void   setCurPage(PageId pageNo)
    {

    }

    public void   setNextPage(PageId pageNo)
    {

    }

    public void   setPrevPage(PageId pageNo)
    {

    }

    public byte[]  getBMpageArray()
    {
        return new byte[1];
    }

    public void  writeBMPageArray(byte[] a)
    {

    }

}