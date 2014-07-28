package org.geworkbench.service.aracne.ws;

import java.rmi.RemoteException;

import javax.xml.bind.JAXBElement;

import org.geworkbench.service.aracne.schema.AracneConfig;
import org.geworkbench.service.aracne.schema.AracneInput;
import org.geworkbench.service.aracne.schema.AracneOutput;
import org.geworkbench.service.aracne.schema.ObjectFactory;
import org.geworkbench.service.aracne.service.AracneInputRepository;
import org.springframework.util.Assert;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;

@Endpoint
public class AracneInputRepositoryEndpoint {
    private AracneInputRepository aracneInputRepository;

    private ObjectFactory objectFactory;

    public AracneInputRepositoryEndpoint(AracneInputRepository aracneInputRepository) {
        Assert.notNull(aracneInputRepository, "'aracneInputRepository' must not be null");
        this.aracneInputRepository = aracneInputRepository;
        this.objectFactory = new ObjectFactory();
    }

    @PayloadRoot(localPart = "ExecuteAracneRequest", namespace = "http://www.geworkbench.org/service/aracne")
    @ResponsePayload
    public JAXBElement<AracneOutput> executeAracne(@RequestPayload JAXBElement<AracneInput> requestElement) throws RemoteException {
        AracneOutput output = new AracneOutput();
    	AracneInput request = requestElement.getValue();

        String runId = aracneInputRepository.storeAracneInput(request);
        
       	output = aracneInputRepository.execute(runId, request.getBootstrapNumber(), request.getMode(), request.getAlgorithm());
        
        return objectFactory.createExecuteAracneResponse(output);
    }

    @PayloadRoot(localPart = "ExecuteAracneConfigRequest", namespace = "http://www.geworkbench.org/service/aracne")
    @ResponsePayload
    public JAXBElement<AracneConfig> executeAracneConfig(@RequestPayload JAXBElement<AracneInput> requestElement) throws RemoteException {
        AracneConfig output = new AracneConfig();
    	AracneInput request = requestElement.getValue();

        String runId = aracneInputRepository.storeConfigInput(request);
        
       	output = aracneInputRepository.executeConfig(runId, request.getAlgorithm());
        
        return objectFactory.createExecuteAracneConfigResponse(output);
    }

}
