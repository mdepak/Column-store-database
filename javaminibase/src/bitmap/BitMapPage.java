package bitmap;

public class BitMapPage extends HFPage {

    int keyType;

    public BitMapPage() {
        // TODO Auto-generated constructor stub
    }

    public BitMapPage(Page page, int keyType)
            throws IOException, ConstructPageException {
        super(page);
        this.keyType = keyType;
    }

    int   available_space()
    {

    }

    void   dumpPage()
    {

    }

    boolean empty()
    {

    }

    void   init(PageId pageNo, Page apage)
    {

    }

    void   openBMpage(Page apage)
    {

    }

    PageId  getCurPage()
    {

    }

    PageId  getNextPage()
    {

    }

    PageId  getPrevPage()
    {

    }

    void   setCurPage(PageId pageNo)
    {

    }

    void   setNextPage(PageId pageNo)
    {

    }

    void   setPrevPage(PageId pageNo)
    {

    }

    byte[]  getBMpageArray()
    {

    }

    void    writeBMPageArray(byte[])
    {

    }

}