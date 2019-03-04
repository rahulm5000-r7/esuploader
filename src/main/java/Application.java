import java.io.IOException;

public class Application {

    public static void main(String[] args) throws IOException, InterruptedException
    {
//        Uploader uploader = new Uploader();
//        uploader.start();
        Consumer cons = new Consumer();
        cons.start();
    }
}
