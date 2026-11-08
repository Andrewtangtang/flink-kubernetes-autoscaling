package org.apache.flink.autoscaler.utils.justin;


import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for REST request to patch a job. */
public class JustinResourceRequirementsUpdateHeaders
        implements RuntimeMessageHeaders<
        JustinResourceRequirementsBody, EmptyResponseBody, JobMessageParameters> {

    public static final JustinResourceRequirementsUpdateHeaders INSTANCE =
            new JustinResourceRequirementsUpdateHeaders();

    private static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/justin";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.PUT;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Request to update job's resource requirements.";
    }

    @Override
    public Class<JustinResourceRequirementsBody> getRequestClass() {
        return JustinResourceRequirementsBody.class;
    }

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }

    @Override
    public String operationId() {
        return "updateJobResourceRequirements";
    }
}