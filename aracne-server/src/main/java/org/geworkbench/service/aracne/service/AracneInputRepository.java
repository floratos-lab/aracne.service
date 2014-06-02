package org.geworkbench.service.aracne.service;

import java.rmi.RemoteException;

import org.geworkbench.service.aracne.schema.AracneConfig;
import org.geworkbench.service.aracne.schema.AracneInput;
import org.geworkbench.service.aracne.schema.AracneOutput;

public interface AracneInputRepository {
    String storeAracneInput(AracneInput input) throws RemoteException;
    String storeConfigInput(AracneInput input) throws RemoteException;

    AracneOutput execute(String dataDir, int nboot, String mode) throws RemoteException;
    AracneConfig executeConfig(String dataDir) throws RemoteException;

}
