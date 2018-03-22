package bitmap;

import java.io.IOException;
import java.lang.*;


import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.ValueClass;
import columnar.*;
import diskmgr.Page;
import java.util.Arrays;

import btree.IntegerKey;
import columnar.ValueClass;
import columnar.*;
import global.Convert;

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;


public class BitMapFile
{

    private PageId headerPageId;
    public Columnarfile columnfile;
    public int columnNo;
    public ValueClass value;
    public String fileName;
    private BitMapHeaderPage headerPage;
    private String dbname;
    private Heapfile bitmapFile;

    BitMapFile(String filename) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        //super(filename);
                headerPageId = get_file_entry(filename);
                headerPage = new BitMapHeaderPage(headerPageId);
                dbname = new String(filename);
            }

    BitMapFile(String filename, Columnarfile columnfile,
            int columnNo, ValueClass value) throws IOException, HFDiskMgrException, HFBufMgrException, HFException, SpaceNotAvailableException, InvalidSlotNumberException, InvalidTupleSizeException {
        //super(filename);
        headerPageId = get_file_entry(filename);
        if (headerPageId == null) {
            headerPage = new BitMapHeaderPage();
            headerPageId = headerPage.getPageId();
            headerPage.setColumnFile(columnfile);   // create in Header page
            headerPage.setColumnNo(columnNo);
            headerPage.setValue(value);
            headerPage.setFileName(filename);
            createBitMapIndex();
        } else {
            // Yet to decide
        }

    }

    private boolean createBitMapIndex()
            throws java.io.IOException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException
    {
        Scan cfs = this.columnfile.openColumnScan(this.columnNo);// columnarFileScan instead of scan
        RID rid = new RID();
        Tuple tScan = new Tuple();
        byte[] yes = new byte[2];
        byte[] no = new byte[2];
        Convert.setCharValue('0', 0, no); //default value is also 0 I guess, could this be of any issue?
        Convert.setCharValue('1', 0, yes);
        byte [] byteValue = null;

        if(this.value instanceof IntegerValue){
            int val = (int) this.value.getValue();
            byteValue = new byte[4];
            Convert.setIntValue(val, 0, byteValue);
        }
        else if(this.value instanceof StringValue){
            String val = (String) this.value.getValue();
            byteValue = new byte[2000];   //need to figure out string size interms of bytes.
            Convert.setStrValue(val, 0, byteValue);
         }
        else if(this.value instanceof FloatValue){
            float val = (float) this.value.getValue();
            byteValue = new byte[4];
            Convert.setFloValue(val, 0, byteValue);
        }
        while((tScan = cfs.getNext(rid))!=null){
            byte[] cData=tScan.getData(); // need to check this..
            if(Arrays.equals(byteValue,cData))
            {
                this.bitmapFile.insertRecord(yes);
            }
            else
            {
                this.bitmapFile.insertRecord(no);
            }
        }
        cfs.closescan();
        return true;
    }
    private PageId get_file_entry(String filename)
    // throws GetFileEntryException
    {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            // throw new GetFileEntryException(e, "");
        }
        return null;
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



        void close() throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
        {
            if ( headerPage!=null) {
                SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
                headerPage = null;
            }
        }

    void destroyBitMapFile()
    {

    }

    void createBitMapFile(BitMapHeaderPage headerPage, HFPage heapPage)
    {

    }

    BitMapHeaderPage getHeaderPage()
    {
        return headerPage;
    }

    boolean Delete(int position)
    {
        return false;
    }

    boolean Insert(int position)
    {
        return false;
    }

}

