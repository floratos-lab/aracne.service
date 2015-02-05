package org.geworkbench.service.aracne.ws;

import java.rmi.RemoteException;

import javax.xml.bind.JAXBElement;

import org.geworkbench.service.aracne.schema.AracneInput;
import org.geworkbench.service.aracne.schema.AracneOutput;
import org.geworkbench.service.aracne.schema.ObjectFactory;
import org.geworkbench.service.aracne.service.Discovery;
import org.springframework.util.Assert;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;

@Endpoint
public class DiscoveryEndpoint {
    private Discovery discovery;

    private ObjectFactory objectFactory;

    public DiscoveryEndpoint(Discovery discovery) {
        Assert.notNull(discovery, "'discovery' must not be null");
        this.discovery = discovery;
        this.objectFactory = new ObjectFactory();
    }

    @PayloadRoot(localPart = "DiscoveryRequest", namespace = "http://www.geworkbench.org/service/aracne")
    @ResponsePayload
    public JAXBElement<AracneOutput> executeAracne(@RequestPayload JAXBElement<AracneInput> requestElement) throws RemoteException {
    	AracneInput request = requestElement.getValue();
    	AracneOutput output = discovery.execute(request);
        
        return objectFactory.createExecuteAracneResponse(output);
    }
}
