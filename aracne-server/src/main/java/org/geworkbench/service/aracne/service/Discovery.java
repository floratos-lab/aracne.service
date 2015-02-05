package org.geworkbench.service.aracne.service;

import java.rmi.RemoteException;

import org.geworkbench.service.aracne.schema.AracneInput;
import org.geworkbench.service.aracne.schema.AracneOutput;

public interface Discovery {

    AracneOutput execute(AracneInput input) throws RemoteException;
}
