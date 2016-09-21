/**
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 * <p>
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.inbound.amazonsqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import com.amazonaws.services.sqs.model.Message;
import org.wso2.carbon.utils.xml.StringUtils;

import java.util.List;
import java.util.Properties;

public class AmazonSQSPollingConsumer extends GenericPollingConsumer {

    private static final Log logger = LogFactory.getLog(AmazonSQSPollingConsumer.class.getName());

    private int waitTime;
    private int maxNoOfMessage;
    private String secretKey;
    private String accessKey;
    private BasicAWSCredentials credentials;
    private AmazonSQS sqsClient;
    private boolean isConnected;
    private String destination;
    private ReceiveMessageRequest receiveMessageRequest;
    private String messageReceiptHandle;

    public AmazonSQSPollingConsumer(Properties amazonsqsProperties, String name,
                                    SynapseEnvironment synapseEnvironment, long scanInterval,
                                    String injectingSeq, String onErrorSeq, boolean coordination,
                                    boolean sequential) {

        super(amazonsqsProperties, name, synapseEnvironment, scanInterval, injectingSeq,
                onErrorSeq, coordination, sequential);
        this.injectingSeq = injectingSeq;
        setUpParameters(amazonsqsProperties);
        logger.info("Initialized the AmazonSQS inbound consumer.");
    }

    /**
     * Load needed parameters from the AmazonSQS inbound endpoint.
     *
     * @param properties the AmazonSQS properties.
     */
    private void setUpParameters(Properties properties) {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting to load the AmazonSQS credentials");
        }
        if (!StringUtils.isEmpty(properties.getProperty(AmazonSQSConstant.AMAZONSQS_SQS_WAIT_TIME))) {
            this.waitTime = Integer.parseInt(properties
                    .getProperty(AmazonSQSConstant.AMAZONSQS_SQS_WAIT_TIME));
        } else {
            this.waitTime = 0;
        }
        if (!StringUtils.isEmpty(properties
                .getProperty(AmazonSQSConstant.AMAZONSQS_SQS_MAX_NO_OF_MESSAGE))) {
            this.maxNoOfMessage = Integer.parseInt(properties
                    .getProperty(AmazonSQSConstant.AMAZONSQS_SQS_MAX_NO_OF_MESSAGE));
        } else {
            this.maxNoOfMessage = 1;
        }
        this.destination = properties.getProperty(AmazonSQSConstant.DESTINATION);
        this.accessKey = properties.getProperty(AmazonSQSConstant.AMAZONSQS_ACCESSKEY);
        this.secretKey = properties.getProperty(AmazonSQSConstant.AMAZONSQS_SECRETKEY);
        if (StringUtils.isEmpty(destination)) {
            throw new QueueDoesNotExistException("URL for the AmazonSQS Queue is empty");
        }
        if (StringUtils.isEmpty(accessKey)) {
            throw new AmazonSQSException("Accesskey is empty");
        }
        if (StringUtils.isEmpty(secretKey)) {
            throw new AmazonSQSException("Secretkey is empty");
        }
        if (waitTime < 0 || waitTime > 20) {
            throw new AmazonSQSException("Value " + waitTime + " for parameter WaitTimeSeconds is invalid. Must be >= 0 and <= 20");
        }
        if (maxNoOfMessage < 1 || maxNoOfMessage > 10) {
            throw new AmazonSQSException("Value " + maxNoOfMessage + " for parameter MaxNumberOfMessages is invalid. Must be between 1 and 10");
        }

        receiveMessageRequest = new ReceiveMessageRequest(destination);
        receiveMessageRequest.withMaxNumberOfMessages(maxNoOfMessage);
        receiveMessageRequest.withWaitTimeSeconds(waitTime);

        if (logger.isDebugEnabled()) {
            logger.debug("Loaded the AmazonSQS AccessKey : " + accessKey
                    + " , SecretKey : " + secretKey);
        }
    }

    /**
     * Create connection with broker and retrieve the messages. Then inject
     * according to the registered handler
     */
    public Message poll() {
        if(logger.isDebugEnabled()) {
            logger.debug("Polling AmazonSQS messages.");
        }
        try {
            if (!isConnected) {
                credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
                sqsClient = new AmazonSQSClient(this.credentials);
                isConnected = true;
            }
            if (sqsClient == null) {
                logger.error("Inbound AmazonSQS endpoint unable to get a connection.");
                isConnected = false;
                return null;
            }

            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
            if (!messages.isEmpty()) {
                for (Message message : messages) {

                        boolean inject;
                        if (logger.isDebugEnabled()) {
                            logger.debug("Injecting AmazonSQS message to the sequence : "
                                    + injectingSeq);
                        }
                        inject = injectMessage(message.toString(), AmazonSQSConstant.CONTENT_TYPE);
                        if (inject) {
                            messageReceiptHandle = message.getReceiptHandle();
                            sqsClient.deleteMessage(new DeleteMessageRequest(destination,//TODO
                                    messageReceiptHandle));
                        }
                }
            } else {
                return null;
            }
        } catch (AmazonServiceException e) {
            throw new AmazonServiceException("Caught an AmazonServiceException, which means your " +
                    "request made it to Amazon SQS, but was rejected with an" +
                    "error response for some reason.", e);
        } catch (AmazonClientException e) {
            throw new AmazonClientException("Caught an AmazonClientException, which means the client" +
                    " encountered a serious internal problem while trying to communicate with SQS, " +
                    "such as not being able to access the network.", e);
        }
        return null;
    }

    public void destroy() {
        try {
            if (sqsClient != null) {
                sqsClient.shutdown();
                if (logger.isDebugEnabled()) {
                    logger.debug("The AmazonSQS has been shutdown !");
                }
            }
        } catch (Exception e) {
            logger.error("Error while shutdown the AmazonSQS" + e.getMessage(), e);
        }
    }

}