package bitmap;

public class BM implements GlobalConst
{
    public BM()
    {

    }
    public static void printBitMap(BitMapHeaderPage header)
    {
        if (header.get_rootId().pid == INVALID_PAGE) {
            System.out.println("The Tree is Empty!!!");
            return;
        }
    }

}