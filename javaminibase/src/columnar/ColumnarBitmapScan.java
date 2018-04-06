package columnar;

public class ColumnarBitmapScan {


  //TODO: add parameters to match reqs
  public ColumnarBitmapScan(String bitmapFile) {

    //Open the appropriate bitmap file and scan the bit map file by following the directory structure of the bitmap file

  }


  /*
 Steps:
 1) Use the directory structure of the BitMap file for traversing
 2) Load the BMPage and get the entire byte array and keep it as local variable in this class
 3) On calling next, maintain a pointer in the byte array and return the data by constructing the tupele
 4) When the pointer of the byte array reaches the end of data, fetch the next BM Page from the disk and keep it in memory
  */
  public Boolean get_next(int position) {

    //Return 'null' in case of end of bit map file

    // Return the tuple.

    return false;
  }

  // Unpin the dir page - used for traversing
  public void close() {

  }

}
