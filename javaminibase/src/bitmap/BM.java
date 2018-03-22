package bitmap;

import global.Convert;
import global.GlobalConst;
import global.RID;
import heap.Scan;
import heap.Tuple;

import java.util.Arrays;

public class BM implements GlobalConst {
    public BM() {

    }

    public static void printBitMap(BitMapHeaderPage header) {
        try
        {
            Scan scanHf= header.columnFile.openColumnScan(header.columnNo);
            RID rid=new RID();
            Tuple tScan=new Tuple();
            byte[] yes=new byte[2];
            byte[] no=new byte[2];
            Convert.setCharValue('0', 0, no);
            Convert.setCharValue('1', 0, yes);
            System.out.println("Printing Bitmap contents: ");
            int cnt=1;
            while((tScan=scanHf.getNext(rid))!=null)
            {
                byte[] temp=tScan.getTupleByteArray();
                if(Arrays.equals(temp,yes))
                {
                    System.out.println("bitmap value at position '"+cnt+"': 1");
                }
                else if(Arrays.equals(temp,no))
                {
                    System.out.println("bitmap value at position '"+cnt+"': 0");
                }
                cnt++;
            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}


