package bitmap;

import java.io.IOException;
import java.lang.*;
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
    public int ColumnNo;
    public ValueClass value;
    public String fileName;
    private BitMapHeaderPage headerPage;
    private String dbname;

    private Heapfile bitmapFile;

    public BitMapFile(String filename)
    {
        headerPageId = get_file_entry(filename);
        headerPage = new BitMapHeaderPage(headerPageId);
        dbname = new String(filename);
    }

    public BitMapFile(String filename, Columnarfile columnfile,
                      int ColumnNo, ValueClass value)
            throws IOException, HFException, HFBufMgrException, HFDiskMgrException,InvalidTupleSizeException,SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException
    {
        headerPageId = get_file_entry(filename);
        if(headerPageId == null)
        {
            this.columnfile = columnfile;    // create in Header page
            this.ColumnNo = ColumnNo;
            this.value = value;
            this.fileName = filename;
            this.bitmapFile = new Heapfile(fileName);
            createBitMapIndex();
        }
        else
        {
            // Yet to decide
        }
    }

    private boolean createBitMapIndex()
            throws java.io.IOException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException
    {
        Scan cfs = this.columnfile.openColumnScan(this.ColumnNo);// columnarFileScan instead of scan
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

            byte[] cData=tScan.getTupleByteArray(); // need to check this..
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

    void close()
    {

    }

    void destroyBitMapFile()
    {

    }

    void createBitMapFile(BitMapHeaderPage headerPage, HFPage heapPage)
    {

    }

    BitMapHeaderPage getHeaderPage()
    {
        return null;
    }

    boolean Delete(int position)
    {
        return false;
    }

    public boolean insert(int position)
    {
        return false;
    }

    void setCurPage_forGivenPosition(int Position)
    {

    }
}