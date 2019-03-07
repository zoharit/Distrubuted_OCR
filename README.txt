****************************************************************
 Distrebuted System Programming 2019 
Assignment 1

or hayat 312198377
zoharith hadad 312211162
****************************************************************


1. Instructions how to run the project.
2. Explanations
3. Security
4. Scalability
5. Persistence
_____________
1. Running instructions:
	java -jar Localapp <input image><amount of images per worker>
	the output will be 
_____________
2. Explanations:
	when the localapp start it upload the input image to s3 
	check if the manager is not active if it  is not active it start the manager instance 
	,create sqs qeue for the output image
	and send a request to the mannager that contain
	 the following info:1.the bucket name of the image
		                2. the image path inside the bucket
		                 3. the output queue url
	after that the local application block untill it recive the output from the queue and it download the result image and save it
	with the name "output"+<input image name>
	

	The manager:
	when the manage start it create 4  executors 
	first one is for client request
	second one is for  sending sqs request when reading input file
	third one is for downloading and creating the html file
	 after initalztion it l continously check the
	manager queue for new client requests and when reciving request submitting them to the threadpool to be
	executed.
	each task in the pool start by  generating client id, counting the number of lines  in the file and for each line creating task 
	in the uplod files executor that send sqs message for the client queue
	after coutning all the lines the task create the requested amount of workers per image
	after creating all the workers the task continusly ask the  workers for result and for each result it create  download task
	that will take care of adding the result to the output file and sending it to the client if it the last request.	
	after creating enough tasks for all the results the client task end.

	
	The worker:
	the worker get as input the input queue url.
	it continously ask for input from the queue when it get input message
	it contains: 1. image url
	 	   2. output queue url
	the client downlaod the image to file and appy ocr then send the response to the output queue url
	that contain:1. image url
		     2. ocr result.

_____________
3. Security

	Safe instances: the instances are only accesible with private key that we have set on the instances
	we use aws roles in order to allow ec2 instances to get temporary security credentials from aws instead of using
	permanant secuirty key.
_____________
4. Scalability
the program scale well  to deal with many clients.
however it is limited by the power of the manager pc so no matter how strong manager you will have there is scalibility limit
to the program.
we also have limit of  only 19 workers instances.

5.Presistence

we have set  a pretty long visability time on the sqs queues in order to send the same message twice if worker 
get stuck for some reason.
