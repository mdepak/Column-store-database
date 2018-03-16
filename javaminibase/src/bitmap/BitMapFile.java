package bitmap;

import java.lang.*;
import columnar.ValueClass;
import columnar.*;
import java.lang.*;
import heap.*;
import bitmap.BitMapHeaderPage;

public class BitMapFile extends Heapfile, HFPage
{
    public BitMapFile(String filename)
    {

    }

    public BitMapFile(String filename, Columnarfile columnfile,
                      int ColumnNo, ValueClass value)
    {

    }

    void close()
    {

    }

    void destroyBitMapFile()
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