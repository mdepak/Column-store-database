package columnar;

import global.TID;
import heap.Tuple;

public class TupleScan {
  TupleScan(Columnarfile cf)
  {
    //TODO: Implement it
  }

  /**
   * Closes the TupleScan object
   */
  void closetuplescan()
  {

  }

  /**
   *   Retrieve the next tuple in a sequential scan
   * @param tid
   * @return
   */
  Tuple getNext(TID tid)
  {
    return null;
  }

  /**
   * Position all scan cursors to the records with the given rids
   * @param tid
   * @return
   */
  boolean position(TID tid)
  {
    return false;
  }
  
}
