import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import dsp.common;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ManagerTask extends  Thread {
    private  List<Instance> all_workers;
    AmazonSQS sqs;
    String result_queue_url;
    String worker_input_url;
    private  AmazonS3 s3;
    private   int n;
    String ID;
    private   AmazonEC2 ec2;
    String Client_url;
    String bucket_id;
    private String image_key;
    AtomicLong LinesCounter;
    File file;
    String title;
    final PrintWriter writer;
    private AtomicBoolean done;
    ManagerTask(JSONObject o,long id) throws FileNotFoundException {
        all_workers = new CopyOnWriteArrayList<>();
        done=new AtomicBoolean(false);
        this.file= new File("tmp"+id+".html");
        writer = new PrintWriter(file);

        LinesCounter=new AtomicLong(0);
        this.sqs = manager.sqs;
        this.s3 = manager.s3;
        this.title=o.getString("title");

        Client_url = o.getString("url");//res url
        bucket_id = o.getString("bucket_id");
        image_key = o.getString("imagekey");

        writer.write("<html>\n" +
                "    <head>\n" +
                "        <title>"+this.title+"</title>\n" +
                "    </head>\n" +
                "    <body>");
        n = o.getInt("imagesPerWorker");
        this.ec2=manager.ec2;
        Date today = new Date();
        ID=(today.toString()+ UUID.randomUUID().toString()).replaceAll("\\s+","-").replace('\\', '-').replace('/', '-').replace(':', '-');
        String name="img"+ID;
        worker_input_url=common.initQueues(sqs,name,"350");
        System.out.println("created worker inputurl   " );
        name="result"+ID;
        result_queue_url=common.initQueues(sqs,name,"350");

    }

    private  String join(Collection<String> s) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append("\n");
        }
        return builder.toString();
    }
    private String getUserDataScript(){
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash");
        lines.add("sudo su");
        lines.add("cd /home/ec2-user");
        lines.add("java8 -cp .:worker.jar worker  "+ID);

        return new String(Base64.encodeBase64(join(lines).getBytes()));
    }

    private void Create_worker()
    {
        System.out.println("creating worker lambda");
            System.out.println("start create worker EC2");
            String MANAGER_ARN = "arn:aws:iam::275240463639:instance-profile/ass1role";
            RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                    .withImageId("ami-086ecbbba55b9a13c")
                    .withInstanceType(InstanceType.T2Micro)
                    .withMinCount(1)
                    .withMaxCount(1)
                    .withUserData(getUserDataScript()).withKeyName("key2")
                    .withIamInstanceProfile(new IamInstanceProfileSpecification()
                            .withArn(MANAGER_ARN)
                    ).withTagSpecifications(new TagSpecification()
                            .withResourceType(ResourceType.Instance)
                            .withTags(new Tag()
                                    .withKey("keyProc")
                                    .withValue("worker")));
            List<Instance> instances = ec2.runInstances(runInstancesRequest).getReservation().getInstances();
            all_workers.addAll(instances);
    }


    void terminateAllWorkers() {
        int instanceCount = 0;
        List<String> instanceIds = new ArrayList<>();
        for (Instance instance : all_workers)
        {
            if (!instance.getState().getName().equals(InstanceStateName.Running.toString())
                    &&!instance.getState().getName().equals(InstanceStateName.Pending.toString()))
            {
                System.out.println("not running"  +instance.getState().getName());
                continue;
            }
            instanceIds.add(instance.getInstanceId());
            System.out.println("found worker"+instanceCount);
            instanceCount++;
        }
        System.out.println("\n\n\n\nlength"+instanceIds.size()+"\n\n\n\n");
        if (instanceIds.size()>0) {
            TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest(instanceIds);
            TerminateInstancesResult terminateInstancesResult = ec2.terminateInstances(terminateInstancesRequest);
            List<InstanceStateChange> changedInstances = terminateInstancesResult.getTerminatingInstances();
            for (InstanceStateChange ins : changedInstances)
                System.out.println("Instance id: " + ins.getInstanceId() + " changed from " + ins.getPreviousState()
                        + " to " + ins.getCurrentState());
        }
    }



    public void run() {




        S3Object object = common.downloadObject(s3, bucket_id, image_key);
        System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
        try {
            send_task(object.getObjectContent());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void send_task(InputStream input) throws IOException {
       try {
           BufferedReader reader = new BufferedReader(new InputStreamReader(input));
           int counter = 0;
           AtomicInteger worker_count = new AtomicInteger(0);
           AtomicInteger LinesCounter2 = new AtomicInteger(0);
           while (true) {
               System.out.println("readed line " + counter);
               String line = reader.readLine();
               if (line == null) {
                   break;
               } else {
                   LinesCounter.getAndIncrement();
                   LinesCounter2.getAndIncrement();
                   JSONObject o = new JSONObject();
                   o.put("MessageType", "new image task");
                   o.put("image_url", line);
                   o.put("result_queue", result_queue_url);
                   o.put("id", LinesCounter2.get());

                   counter = (counter + 1) % n;
                   upload_task up = new upload_task(o, worker_input_url);
                   manager.upload_pool.submit(up);

                   if (counter == 0) {
                       worker_count.incrementAndGet();
                   }
               }
           }
           if (counter != 0) {
               worker_count.incrementAndGet();
           }
           System.out.println("worker count = " + worker_count.get() + "worker counter");
           create_workers(worker_count.get());
           done.getAndSet(true);
           System.out.println("got" + LinesCounter + " lines");
           System.out.println("Linecounter = " + LinesCounter.get() + "done = " + done.get());
           while (LinesCounter2.get() > 0 || !done.get()) {
               List<Message> messages = common.receiveMessage(sqs, result_queue_url);
               for (Message msg : messages) {
                   System.out.println("received message" + msg.getBody());
                   LinesCounter2.decrementAndGet();
                   Download_task download_task = new Download_task(this, msg);
                   manager.download_pool.submit(download_task);
               }
           }
       }
       catch (Exception e){
           e.printStackTrace();
       }

    }

    private void create_workers(int i) {
        System.out.println("creating workers");
        for(int k=0;k<i;k++)
        {
            System.out.println("creating worker task");
            Create_worker();
        }
    }
}

