package com.nifi.anonymization.processor;

import com.nifi.anonymization.JsonAnonymize;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.util.*;

@Tags({"custom", "anonymize", "json", "HR custom processor"})
@CapabilityDescription("Anonymize fields in the flowfile. Fields names and types are described in the Attributes list property")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class JsonAnonymizeProcessor extends AbstractProcessor {
    public static final PropertyDescriptor PROP_ATTRIBUTES_LIST = new PropertyDescriptor
            .Builder().name("Attributes List")
            .description("Json object containing a list of fields to anonymize (with the name of the key, and the type of the value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptorsList = new ArrayList<PropertyDescriptor>();
        descriptorsList.add(PROP_ATTRIBUTES_LIST);
        this.descriptors = Collections.unmodifiableList(descriptorsList);

        final Set<Relationship> relationshipsSet = new HashSet<Relationship>();
        relationshipsSet.add(REL_SUCCESS);
        relationshipsSet.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationshipsSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        return;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog logger = getLogger();
        try {

            final JSONArray attrToAnonymize;
            try {
                attrToAnonymize = new JSONArray(context.getProperty(PROP_ATTRIBUTES_LIST).evaluateAttributeExpressions(flowFile).getValue());
            } catch (JSONException e) {
                throw new ProcessException(e + " Invalid Attributes List to anonymize");
            }

            final ByteArrayOutputStream inputContent = new ByteArrayOutputStream();
            session.exportTo(flowFile, inputContent);
            JSONObject json;
            try {
                json = new JSONObject(inputContent.toString());
            } catch (JSONException e) {
                throw new ProcessException(e + " Flowfile content is not a valid JSON");
            }

            //Process the flowfile content
            String jsonOuput = JsonAnonymize.anonymize(json, attrToAnonymize);

            flowFile = session.write(flowFile,
                    outputStream -> outputStream.write(jsonOuput.getBytes("UTF-8"))
            );
            session.transfer(flowFile, REL_SUCCESS);

        } catch (ProcessException | JSONException e) {
            logger.error(e + " Unable to anonymize flowfile");
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
