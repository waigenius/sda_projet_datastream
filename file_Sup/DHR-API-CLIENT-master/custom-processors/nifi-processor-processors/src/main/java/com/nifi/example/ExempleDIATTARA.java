package com.nifi.example;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.json.JSONException;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"example", "custom", "processor", "nifi"})
@CapabilityDescription("Transforms input data from format 'diattara,15' to JSON format {'nom':'diattaar', 'age':15}.")
public class ExempleDIATTARA extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Successfully transformed input data.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failed to process input data.")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog logger = getLogger();
        try {

            final ByteArrayOutputStream inputContent = new ByteArrayOutputStream();
            session.exportTo(flowFile, inputContent);
            String result;
            try {
                String str = inputContent.toString();
                String[] parts = str.split(",");
                String nom = parts[0];
                int age = Integer.parseInt(parts[1]);
                result = String.format("{\"nom\":\"%s\", \"age\":%d}", nom, age);
            } catch (JSONException e) {
                throw new ProcessException(e + " Flowfile content is not good");
            }
            flowFile = session.write(flowFile,
                    outputStream -> outputStream.write(result.getBytes("UTF-8"))
            );
            session.transfer(flowFile, SUCCESS);

        } catch (ProcessException | JSONException e) {
            logger.error(e + " Unable to parser content");
            session.transfer(flowFile, FAILURE);
        }
    }

}