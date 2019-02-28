import java.io.IOException;

public class Application {

    public static void main(String[] args) throws IOException {
        Uploader uploader = new Uploader();
        uploader.start();
    }
}
