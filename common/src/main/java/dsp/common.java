package dsp;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
        import com.amazonaws.services.s3.model.S3Object;
        import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class common {

    public static String MANAGER_NAME="manager";

    public static String  upload_file(AmazonS3 s3, String bucket_name, String path, String file_name, File file) {
        String key = path + file_name.replace('\\', '-').replace('/', '-').replace(':', '-');

        System.out.println("uploading file" + key);
        s3.putObject(bucket_name, key, file);
        System.out.println("uploaded file successfully");
        return key;
    }



    public static String initQueues(AmazonSQS sqs,String queue_name,String visability_timeout) {
        String url_res="";
        boolean queuesExists = true;
        GetQueueUrlResult response1 = null;
        GetQueueUrlRequest request1 = new GetQueueUrlRequest().withQueueName(queue_name);
        //               .withQueueOwnerAWSAccountId(accountID);
        try
        {
            response1 = sqs.getQueueUrl(request1);
            url_res = response1.getQueueUrl();
        } catch (QueueDoesNotExistException e)
        {
            System.out.println(" Queue does not exists!");
            queuesExists = false;
        }
        if (queuesExists)
        {
            System.out.println("LocalApp Queues already exists");
            System.out.println("Getting url of LocalApp queues: " + response1.toString());
        } else {
            try {
                // Create a queue
                System.out.println("Creating a new SQS queue called filesQueue.\n");
                Map<String, String> attributes = new HashMap<>();
               // attributes.put("FifoQueue", "true");
                //attributes.put("ContentBasedDeduplication", "true");
                attributes.put(QueueAttributeName.VisibilityTimeout.toString(),visability_timeout);
                 CreateQueueRequest createFilesQueueRequest;
                createFilesQueueRequest = new CreateQueueRequest(queue_name).withAttributes(attributes).addAttributesEntry(QueueAttributeName.ReceiveMessageWaitTimeSeconds
                        .toString(), "20");
                url_res = sqs.createQueue(createFilesQueueRequest).getQueueUrl();
                common.list_queues(sqs);
                System.out.println();
                return url_res;
            }
            catch (AmazonServiceException ase)
            {
                System.out.println("Caught an AmazonServiceException, which means your request made it " +
                        "to Amazon SQS, but was rejected with an error response for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());
                ase.printStackTrace();
            }
            catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which means the client encountered " +
                        "a serious internal problem while trying to communicate with SQS, such as not " +
                        "being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());
                ace.printStackTrace();
            }
        }
        return url_res;
    }

    public static List<Message> receiveMessage(AmazonSQS sqs,String myQueueUrl){
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    public static void sendMessage (AmazonSQS sqs,String url,String message ){
        try {
            SendMessageRequest sendreq =new SendMessageRequest(url, message);
            sqs.sendMessage(sendreq);
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while sending a message to SQS queue");
            throw ex;
        }
        System.out.println("Message sent to SQS. sqs url:"+url+" queue:\n" + message);
    }

    public static S3Object downloadObject(AmazonS3 s3, String bucketName, String key) {
        S3Object res;
        try {
            res = s3.getObject(bucketName, key);
        } catch (AmazonClientException ex) {
            System.err.println("Caught an exception while downloading S3 object");
            throw ex;
        }
        System.out.println("Downloaded file: " + key + " from S3 bucket: " + bucketName);
        return res;
    }

    private static void list_queues(AmazonSQS sqs){
        // List queues
        System.out.println("Listing all queues in your account.\n");
        for (String queueUrl : sqs.listQueues().getQueueUrls())
        {
            System.out.println("  QueueUrl: " + queueUrl);
        }
    }

    public static boolean createBucket(AmazonS3 s3,String  bucket_name)
    {

        if(s3.doesBucketExistV2(bucket_name))
        {
            System.out.format("Bucket %s already exists.\n", bucket_name);
            return false;
        }
        try
        {

            s3.createBucket(bucket_name);
        } catch (AmazonS3Exception e)
        {
            System.err.println(e.getErrorMessage());
            return false;
        }
        return  true;
    }

}
