package org.apache.flink.autoscaler.utils.justin;


import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for REST request to get details on job's resources. */
public class JustinResourceRequirementsHeaders
        implements RuntimeMessageHeaders<
        EmptyRequestBody, JustinResourceRequirementsBody, JobMessageParameters> {

    public static final JustinResourceRequirementsHeaders INSTANCE =
            new JustinResourceRequirementsHeaders();

    private static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/justin";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<JustinResourceRequirementsBody> getResponseClass() {
        return JustinResourceRequirementsBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Request details on the job's resource requirements.";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }
}