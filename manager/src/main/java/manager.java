import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import dsp.common;
import org.json.JSONObject;

import java.io.FileNotFoundException;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


public class manager {
    static AmazonS3 s3;
    static AmazonSQS sqs;
    static AmazonEC2 ec2;
    private  static AtomicLong id=new AtomicLong(0);
    static ExecutorService upload_pool;
    //static ExecutorService create_worker_pool;
    static ExecutorService download_pool;


    private static void init(){
        InstanceProfileCredentialsProvider credentialsProvider = new InstanceProfileCredentialsProvider(false);
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

    }



    public static void main(String [] args)
    {
        upload_pool = Executors.newFixedThreadPool(4);
        ExecutorService pool = Executors.newFixedThreadPool(5);
        download_pool=Executors.newFixedThreadPool(5);
        init();
        String manager_url = common.initQueues(sqs, common.MANAGER_NAME,"300");
        while(true)
        {
            List<Message> messageList = sqs.receiveMessage(manager_url).getMessages();
            for(Message msg : messageList)
            {
                System.out.println("got message");
                JSONObject json = new JSONObject(msg.getBody());
                ManagerTask t= null;
                try {
                    t = new ManagerTask(json,id.getAndIncrement());
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                sqs.deleteMessage(common.MANAGER_NAME,msg.getReceiptHandle());
                if(t !=null)
                {
                    pool.execute(t);
                }
            }
        }
    }


}
