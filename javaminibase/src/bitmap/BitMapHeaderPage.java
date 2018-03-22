package bitmap;

import columnar.Columnarfile;
import columnar.ValueClass;
import diskmgr.Page;
import global.PageId;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

class BitMapHeaderPage extends HFPage{

    public Columnarfile columnFile;
    public int columnNo;
    public ValueClass value;
    public String fileName;

    void setColumnFile(Columnarfile columnfile) {
        this.columnFile = columnfile;
    }

    Columnarfile getColumnfile()
    {
        return this.columnFile;
    }

    void setColumnNo(int columnNo) {
        this.columnNo = columnNo;
    }

    int getColumnNo()
    {
        return this.columnNo;
    }

    void setValue(ValueClass value) {
        this.value = value;
    }

    ValueClass getValue()
    {
        return this.value;
    }

    void setFileName(String fileName) {
        this.fileName = fileName;
    }

    String getFileName()
    {
        return this.fileName;
    }

    void setPageId(PageId pageno) throws IOException {
        setCurPage(pageno);
    }

    PageId getPageId()
            throws IOException {
        return getCurPage();
    }


/*    void setPageId(PageId pageno)
            throws IOException {
        setCurPage(pageno);
    }

    PageId getPageId()
            throws IOException {
        return getCurPage();
    }

    *//**
     * set the magic0
     *
     * @param magic magic0 will be set to be equal to magic
     *//*
    void set_magic0(int magic)
            throws IOException {
        setPrevPage(new PageId(magic));
    }


    *//**
     * get the magic0
     *//*
    int get_magic0()
            throws IOException {
        return getPrevPage().pid;
    }



    *//**
     * set the rootId
     *//*
    void set_rootId(PageId rootID)
            throws IOException {
        setNextPage(rootID);
    }

    ;

    *//**
     * get the rootId
     *//*
    PageId get_rootId()
            throws IOException {
        return getNextPage();
    }

    *//**
     * set the key type
     *//*
    void set_keyType(short key_type)
            throws IOException {
        setSlot(3, (int) key_type, 0);
    }

    *//**
     * get the key type
     *//*
    short get_keyType()
            throws IOException {
        return (short) getSlotLength(3);
    }

    *//**
     * get the max keysize
     *//*
    void set_maxKeySize(int key_size)
            throws IOException {
        setSlot(1, key_size, 0);
    }

    *//**
     * set the max keysize
     *//*
    int get_maxKeySize()
            throws IOException {
        return getSlotLength(1);
    }

    *//**
     * set the delete fashion
     *//*
    void set_deleteFashion(int fashion)
            throws IOException {
        setSlot(2, fashion, 0);
    }

    *//**
     * get the delete fashion
     *//*
    int get_deleteFashion()
            throws IOException {
        return getSlotLength(2);
    }


    *//**
     * pin the page with pageno, and get the corresponding SortedPage
     */
    public BitMapHeaderPage(PageId pageno)
    {
        super();
        try {

            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        } catch (Exception e) {
           // throw new ConstructPageException(e, "pinpage failed");
        }
    }

    /**
     * associate the SortedPage instance with the Page instance
     */
    public BitMapHeaderPage(Page page) {

        super(page);
    }


    /**
     * new a page, and associate the SortedPage instance with the Page instance
     */
    public BitMapHeaderPage()
            //throws ConstructPageException
    {
        super();
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null) {
               // throw new ConstructPageException(null, "new page failed");
            }
            this.init(pageId, apage);

        } catch (Exception e) {
            //throw new ConstructPageException(e, "construct header page failed");
        }
    }
}