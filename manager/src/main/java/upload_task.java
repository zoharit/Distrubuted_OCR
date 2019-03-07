import dsp.common;
import org.json.JSONObject;

public class upload_task implements  Runnable {

    private JSONObject o;
    private  String worker_input_url;
    upload_task(JSONObject o,String worker_input_url)
    {
        this.o=o;
        this.worker_input_url=worker_input_url;
    }
    @Override
    public void run() {
        System.out.println("uploading file with id ="+o.getInt("id"));
        common.sendMessage(manager.sqs,worker_input_url, o.toString());
    }
}
