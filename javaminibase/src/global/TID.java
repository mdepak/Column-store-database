package global;

public class TID {

  int numRIDs;
  int position;
  RID[] recordIDs;

  public TID(int numRIDs) {
    this.numRIDs = numRIDs;
    RID[] records = new RID[numRIDs];
    for(int i=0; i<numRIDs; i++)
      records[i] = new RID();
    this.recordIDs = records;
  }

  public TID(int numRIDs, int position) {
    this.numRIDs = numRIDs;
    this.position = position;
  }

  public TID(int numRIDs, int position, RID[] recordIDs) {
    this.numRIDs = numRIDs;
    this.position = position;
    this.recordIDs = recordIDs;
  }

  public RID getRID(int fieldNo)
  {
    return recordIDs[fieldNo];
  }

  public int getNumRIDs()
  {
    return numRIDs;
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
    this.position = position;
  }

  /**
   * set the RID of the given column
   */
  public void setRID(int column, RID recordID) {
    recordIDs[column] = recordID;
  }

}
