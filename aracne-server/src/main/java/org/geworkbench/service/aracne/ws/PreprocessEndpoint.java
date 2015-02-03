package org.geworkbench.service.aracne.ws;

import java.rmi.RemoteException;

import javax.xml.bind.JAXBElement;

import org.geworkbench.service.aracne.schema.AracneConfig;
import org.geworkbench.service.aracne.schema.AracnePreprocessInput;
import org.geworkbench.service.aracne.schema.ObjectFactory;
import org.geworkbench.service.aracne.service.Preprocess;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;

@Endpoint
public class PreprocessEndpoint {

    private ObjectFactory objectFactory;

	private Preprocess preprocess;

    public PreprocessEndpoint(Preprocess preprocess) {
        this.preprocess = preprocess;
        this.objectFactory = new ObjectFactory();
    }

    /* new flavor of processing that does not depend on the parameters the ARACNE computation needs */
    @PayloadRoot(localPart = "PreprocessRequest", namespace = "http://www.geworkbench.org/service/aracne")
    @ResponsePayload
    public JAXBElement<AracneConfig> preprocess(@RequestPayload JAXBElement<AracnePreprocessInput> requestElement) throws RemoteException {
        AracnePreprocessInput request = requestElement.getValue();
        AracneConfig output = preprocess.execute(request, request.getAlgorithm());
        
        return objectFactory.createExecuteAracneConfigResponse(output);
    }
}
