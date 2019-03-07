
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2;

import com.amazonaws.services.sqs.model.Message;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;
import dsp.common;

import java.io.*;
import java.util.*;


public class LocalApp {
    private static AmazonEC2 ec2;
    private static AmazonS3 s3;
    private static AmazonSQS sqs;
    private static String MANAGER_ROLE;

    static {
        MANAGER_ROLE = "arn:aws:iam::275240463639:instance-profile/ass1role";
    }

    private static void init(){
        AWSCredentialsProvider credentialsProvider;

        credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());


        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

    }


    private static boolean managerExist(AmazonEC2 ec2){

        for (Reservation reservation : ec2.describeInstances().getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                for (Tag tag : instance.getTags()) {
                    if (tag.getKey().equals("keyProc") &&
                            tag.getValue().equals("manager") &&
                            (instance.getState().getName().
                                    equals(InstanceStateName.Pending.toString()) ||
                                    instance.getState().getName().
                                            equals(InstanceStateName.Running.toString()))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }


    private static String join(Collection<String> s) {
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
    private static String getUserDataScript(){
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash");
        lines.add("sudo su");
        //      lines.add("export _JAVA_OPTIONS=\"-Xms512m -Xmx3024m\"");
        lines.add("cd /home/ec2-user");
       // lines.add("sudo yum remove java-1.7.0 -y");
       // lines.add("sudo yum install java-1.8.0 -y");
      //  lines.add("java -jar manager.jar");
        lines.add("java8 -cp .:manager.jar manager");


        return new String(Base64.encodeBase64(join(lines).getBytes()));
    }

    private static void createManager(AmazonEC2 ec2) {
        System.out.println("start create manager EC2");
        ec2.runInstances(new RunInstancesRequest()
                .withImageId("ami-0e12fff657292492a")
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1).withUserData(getUserDataScript()).withKeyName("key2")
                .withIamInstanceProfile(new IamInstanceProfileSpecification()
                        .withArn(MANAGER_ROLE)
                )
                .withTagSpecifications(new TagSpecification()
                        .withResourceType(ResourceType.Instance)
                        .withTags(new Tag()
                                .withKey("keyProc")
                                .withValue("manager"))));
    }

    private static void startManagerIfNotActive() {
        try {
            if (!managerExist(ec2)) {
                createManager(ec2);
                System.out.println("Manager instance created and started");
            } else {
                System.out.println("Manager instance is already running");
            }
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while creating the ec2 manager node");
            throw ex;
        }
    }


    private static void create_output(String name, S3Object obj) throws IOException {

        InputStream  in;
        OutputStream out;
        System.out.println("create output");
        in = obj.getObjectContent();
        out = new FileOutputStream(name);


        int c;
        while ((c = in.read()) != -1) {
            out.write(c);
        }
        in.close();
        out.close();
    }
    public static void main(String [] args)  {
        try {

        Date today = new Date();

            String ID = (today.toString() + UUID.randomUUID().toString()).replaceAll("\\s+", "-").replace('\\', '-').replace('/', '-').replace(':', '-');
        System.out.println("user id="+ ID);
        if (args.length != 2)
        {
            System.out.printf("bad input format got %d arguments\n", args.length);
            System.exit(-1);
        }


        init();
        int n = Integer.parseInt(args[1]);
        File input_file=new File(args[0]);
        startManagerIfNotActive();

            String bucket_arn = "dsp-191-ocr1";
            String images_key = common.upload_file(s3, bucket_arn, "input/", args[0] + ID, input_file);


            String manager_url = common.initQueues(sqs, common.MANAGER_NAME, "150");
        String clientName="clientUrl"+ ID;
        clientName=clientName.substring(0,Math.min(70,clientName.length()));
        System.out.println("clientname = "+clientName+"        "+clientName.length());
            String client_url = common.initQueues(sqs, clientName, "150");


        JSONObject j = new JSONObject();
        j.put("MessageType", "new task");
        j.put("url", client_url);
        j.put("bucket_id", bucket_arn);
        j.put("imagesPerWorker",n);
        j.put("imagekey", images_key);
        j.put("title",args[0]);
        System.out.println(j.toString());


        common.sendMessage(sqs, manager_url,j.toString());
            System.out.println("client url is  "+ client_url);
        //sqs.sendMessage(ImagesQueueUrl,bucket_id+"{"+n+"{"+images_key);
        int received=0;
        while (received==0)
        {
            List<Message> message=   common.receiveMessage(sqs, client_url);
            for(Message m:message)
            {
                System.out.println("got mesagge from mannager   "+ client_url);
                received++;
                JSONObject body=new JSONObject(m.getBody());
                S3Object obj=common.downloadObject(s3,body.getString("bucket_id"),body.getString("res_key"));
                sqs.deleteMessage(client_url,m.getReceiptHandle());
                try {
                    create_output("output"+args[0]+".html",obj);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        sqs.deleteQueue(client_url);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Response Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }

        }
}
