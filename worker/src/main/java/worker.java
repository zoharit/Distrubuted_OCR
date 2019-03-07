import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import dsp.common;
import com.asprise.ocr.Ocr;

import org.json.JSONObject;
import java.nio.file.Paths;
import java.io.*;
import java.net.URL;
import java.util.List;


public class worker {

    private static AmazonSQS sqs;
    private static long id=0;
    private static Ocr ocr;



    private static void initOCR(){

        Ocr.setUp(); // one time setup
        ocr = new Ocr(); // create a new OCR engine
        ocr.startEngine("eng", Ocr.SPEED_FASTEST); // English
    }


    private static void init(){
         InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(false);

        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

    }



    private static String DownloadImage(String imageUrl) throws IOException {
        URL url = new URL(imageUrl);
        String destinationFile = id+ Paths.get(url.getPath()).getFileName().toString();
        System.out.println(destinationFile);
        try (InputStream is = url.openStream();
             OutputStream os = new FileOutputStream(destinationFile)) {
            byte[] b = new byte[2048];
            int length;

            while ((length = is.read(b)) != -1) {
                os.write(b, 0, length);
            }
        }
        return  destinationFile;
    }

    private static String do_ocr(String url)
    {

        String destinationFile;
        try {
            destinationFile = DownloadImage(url);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return "could not download and save " +url;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return "error url = " +url;
        }


        String recognition = ocr.recognize(
                new File[] {new File(destinationFile)}, Ocr.RECOGNIZE_TYPE_ALL, Ocr.OUTPUT_FORMAT_PLAINTEXT);

        if (recognition.isEmpty()) {
            return "<error: failed to recognize image from url: " + url + ">";
        }
        System.out.println("Result: " + recognition);
        return recognition;

    }



public static void main(String [] args)
{
    init();
    initOCR();

   // System.out.println(do_ocr("http://www.columbiamt.com/CMT-Marking-Stamps/images/OCR-A-Font.gif"));
    String output_url = common.initQueues(sqs, "result" + args[0] ,"150");
    String input_url=common.initQueues(sqs,"img"+args[0],"300");

    while(true) {
        List<Message> msg = common.receiveMessage(sqs, input_url);
        for (Message m : msg) {
            id++;

            JSONObject json = new JSONObject(m.getBody());
            String res = do_ocr(json.getString("image_url"));

            JSONObject o = new JSONObject();
            o.put("MessageType", "done image task");
            o.put("image_url",json.getString("image_url"));
            o.put("result", res);
            o.put("id",json.getLong("id"));
            common.sendMessage(sqs,output_url, o.toString());
            sqs.deleteMessage(input_url, m.getReceiptHandle());
        }
    }
  }

}



