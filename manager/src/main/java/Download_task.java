import com.amazonaws.services.sqs.model.Message;
import dsp.common;
import org.json.JSONObject;

public class Download_task implements Runnable {
    private ManagerTask task;
    private Message msg;

    Download_task(ManagerTask task, Message msg){
        this.task=task;
        this.msg=msg;
    }



    @Override
    public void run() {
        JSONObject json = new JSONObject(msg.getBody());
        synchronized (task.writer) {
            task.writer.write("<p>\n" +
                    "        <img src=\"" + json.getString("image_url") + "\"/>\n" +
                    json.getString("result") + "\n" +
                    "    </p>\n\t");
            long newCounter=  task.LinesCounter.decrementAndGet();
            task.sqs.deleteMessage(task.result_queue_url, msg.getReceiptHandle());
            if(newCounter==0)
            {
                task.writer.write("\t</body>\n" +
                        "</html>");
                task.writer.flush();
                String res_key = common.upload_file(manager.s3, task.bucket_id, "output/", task.title + task.ID + ".html", task.file);
                System.out.println("done downloading");
                task.writer.close();
                JSONObject o = new JSONObject();
                o.put("MessageType", "done task");
                o.put("res_key", res_key);
                o.put("bucket_id", task.bucket_id);
                o.put("id", task.ID);
                common.sendMessage(task.sqs, task.Client_url, o.toString());
                if(task.file.delete())
                {
                    System.out.println("deleted file"+task.file.getName());
                }
                else
                {
                    System.out.println("failed to delete file"+task.file.getName());
                }
                manager.sqs.deleteQueue(task.result_queue_url);
                manager.sqs.deleteQueue(task.worker_input_url);
                task.terminateAllWorkers();
            }
        }
    }
}
