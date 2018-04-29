package bitmap;

import bitmap.BitMapFile;
import bitmap.BitmapScan;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.RID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import iterator.NestedLoopException;
import java.io.IOException;

public class BitmapNestedLoopScan {

  BitmapScan outer;
  BitmapScan inner;

  BitMapFile outerBitmap;
  BitMapFile innerBitmap;

  boolean done = false,  get_from_outer;

  int outerPos, innerPos;


  public BitmapNestedLoopScan(BitmapPair bitmapJoinPair)
      throws InvalidTupleSizeException, IOException, ConstructPageException, GetFileEntryException, HFException, HFBufMgrException, HFDiskMgrException
  {
    String leftBitmapFile = bitmapJoinPair.getLeftBitmapFile();
    String rightBitmapFile = bitmapJoinPair.getRightBitmapFile();

   outerBitmap = new BitMapFile(leftBitmapFile);
   innerBitmap = new BitMapFile(rightBitmapFile);

    get_from_outer = true;

    outer = new BitmapScan(outerBitmap);
    inner = new BitmapScan(innerBitmap);
  }

  Boolean getNext(Boolean outerFilterResult, Boolean innerFilterResult) throws InvalidTupleSizeException, IOException, NestedLoopException {
    Boolean outer_tuple = null;
    Boolean inner_tuple = null;

    if (done) {
      return null;
    }

    do {
      // If get_from_outer is true, Get a tuple from the outer, delete
      // an existing scan on the file, and reopen a new scan on the file.
      // If a get_next on the outer returns DONE?, then the nested loops
      //join is done too.

      if (get_from_outer == true) {
        get_from_outer = false;
        if (inner != null)     // If this not the first time,
        {
          // close scan
          inner = null;

        }

        try {
          inner = new BitmapScan(innerBitmap);
          innerPos = 0;
        } catch (Exception e) {
          throw new NestedLoopException(e, "Inner nested bit map  scan openScan failed");
        }

        while((outer_tuple = outer.getNext(new RID())) == false)
        {
          outerPos++;
        }

        if ((outer_tuple = outer.getNext(new RID())) == null) {
          done = true;
          if (inner != null) {

            inner = null;
          }

          return null;
        }
      }  // ENDS: if (get_from_outer == TRUE)

      // The next step is to get a tuple from the inner,
      // while the inner is not completely scanned && there
      // is no match (with pred),get a tuple from the inner.

      RID rid = new RID();
      while ((inner_tuple = inner.getNext(rid)) != null) {
          innerPos++;
          if(inner_tuple)
          {
            //Construct the tuple and return the data

            return (outerFilterResult && outer_tuple) && (inner_tuple && innerFilterResult);
          }
      }

      // There has been no match. (otherwise, we would have
      //returned from t//he while loop. Hence, inner is
      //exhausted, => set get_from_outer = TRUE, go to top of loop

      get_from_outer = true; // Loop back to top and get next outer tuple.
    } while (true);

  }


  public void close()
      throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {

    outer.closescan();
    inner.closescan();
    outerBitmap.close();
    innerBitmap.close();

  }
}
