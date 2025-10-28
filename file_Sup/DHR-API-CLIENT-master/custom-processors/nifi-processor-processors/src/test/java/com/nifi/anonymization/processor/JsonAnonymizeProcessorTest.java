package com.nifi.anonymization.processor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class JsonAnonymizeProcessorTest {

    private TestRunner runner;
    private JsonAnonymizeProcessor jsonAnonymizeProcessor;


    @Before
    public void init() {
        jsonAnonymizeProcessor = new JsonAnonymizeProcessor();
        runner = TestRunners.newTestRunner(JsonAnonymizeProcessor.class);
    }

    @Test
    public void anonymizeFlowfile(){
        runner.setProperty(JsonAnonymizeProcessor.PROP_ATTRIBUTES_LIST, "[{\"name\":\"test\",\"type\":\"string\"}]");
        runner.enqueue("{\"test\":\"hello World\"}");
        runner.run(1);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(JsonAnonymizeProcessor.REL_SUCCESS);
        MockFlowFile result = runner.getFlowFilesForRelationship(JsonAnonymizeProcessor.REL_SUCCESS).get(0);
        result.assertContentEquals("{\"test\":\"db4067cec62c58bf8b2f8982071e77c082da9e00924bf3631f3b024fa54e7d7e\"}");
    }

    @Test
    public void anonymizeInvalidJsonFlowfile(){
        runner.setProperty(JsonAnonymizeProcessor.PROP_ATTRIBUTES_LIST, "[{\"name\":\"test\",\"type\":\"string\"}]");
        runner.enqueue("invalid json");
        runner.run(1);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(JsonAnonymizeProcessor.REL_FAILURE);
    }

    @Test
    public void anonymizeInvalidJsonProp(){
        runner.setProperty(JsonAnonymizeProcessor.PROP_ATTRIBUTES_LIST, "invalid json");
        runner.enqueue("{\"test\":\"hello World\"}");
        runner.run(1);
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(JsonAnonymizeProcessor.REL_FAILURE);
    }

}