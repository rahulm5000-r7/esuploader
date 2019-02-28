import java.io.IOException;

public class Application {

    public static void main(String[] args) throws IOException {
        System.out.println("System started up");
        Uploader uploader = new Uploader();
        uploader.start();
    }
}
