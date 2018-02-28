package global;

public class TID {

  int numRIDs;
  int position;
  RID[] recordIDs;

  TID(int numRIDs) {
    this.numRIDs = numRIDs;
  }

  TID(int numRIDs, int position) {
    this.numRIDs = numRIDs;
    this.position = position;
  }

  TID(int numRIDs, int position, RID[] recordIDs) {
    this.numRIDs = numRIDs;
    this.position = position;
    this.recordIDs = recordIDs;
  }


  /**
   * make a copy of the given tid
   */
  void copyTid(TID tid) {
    //TODO: Implement it
  }

  /**
   * Compares two TID objects
   */
  boolean equals(TID tid) {
    //TODO: Implement it
    return false;
  }

  /**
   * Write the tid into a byte array at offset
   */
  void writeToByteArray(byte[] array, int offset) {
    //TODO: Implement it
  }

  /**
   * set the position attribute with the given value
   */
  void setPosition(int position){
    //TODO: Implement it
  }

  /**
   * set the RID of the given column
   */
  void setRID(int column, RID recordID) {
    //TODO: Implement it
  }

}
