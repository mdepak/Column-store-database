package bitmap;

import java.lang.*;
import columnar.ValueClass;
import columnar.*;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

public class BitMapFile
    {
    private PageId headerPageId;
    public Columnarfile columnfile;
    public int ColumnNo;
    public ValueClass value;
    public String fileName;
    private BitMapHeaderPage headerPage;
    private String dbname;

    public BitMapFile(String filename)
            {
                headerPageId = get_file_entry(filename);
                headerPage = new BitMapHeaderPage(headerPageId);
                dbname = new String(filename);
            }

    public BitMapFile(String filename, Columnarfile columnfile,
            int ColumnNo, ValueClass value)
            {
                headerPageId = get_file_entry(filename);
                if(headerPageId == null)
                {
                    this.columnfile = columnfile;    // create in Header page
                    this.ColumnNo = ColumnNo;
                    this.value = value;
                    this.fileName = filename;
                }
                else
                {
                    // Yet to decide
                }
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

    boolean Insert(int position)
    {
        return false;
    }

    void setCurPage_forGivenPosition(int Position)
    {

    }
    }