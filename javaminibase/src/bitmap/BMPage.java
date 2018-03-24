package bitmap;

import diskmgr.Page;
import global.PageId;

public class BMPage {
    BMPage(){
        //Default constructor
    }

    BMPage(Page page) {
        //Constructor of class BMPage open a BMPage and make this BMpage point to the given page
    }
    int available_space()
    {
        //returns the amount of available space on the page.
        return 0;
    }

    void dumpPage()
    {
        //Dump contents of a page
    }

    boolean empty()
    {
        //Determining if the page is empty
        return true;
    }

    void init(PageId pageNo, Page apage){
        //    Constructor of class BMPage initialize a new page
    }
    void openBMpage(Page apage){
        //    Constructor of class BMPage open a existed BMPage
    }

    PageId getCurPage(){
        PageId p = new PageId();
        return p;
    }
    PageId getNextPage(){
        PageId p = new PageId();
        return p;
    }
    PageId getPrevPage(){
        PageId p = new PageId();
        return p;
    }
    void setCurPage(PageId pageNo){
        //sets value of curPage to pageNo
    }

    void setNextPage(PageId pageNo){
        //sets value of nextPage to pageNo
    }
    void setPrevPage(PageId pageNo){
        //sets value of prevPage to pageNo
    }

    byte[] getBMPageArray(){
        byte [] a = new byte[0];
        return a;
    }
    void writeBMPageArray(byte[] data){

    }
}
