package org.geworkbench.service.aracne.service;

import java.rmi.RemoteException;

import org.geworkbench.service.aracne.schema.AracneConfig;
import org.geworkbench.service.aracne.schema.AracnePreprocessInput;

/* Aracne preprocessing should be completely separated from Aracne computation.
 * There is no sensible reason that they were implemented under a common interface. */
public interface Preprocess {
    AracneConfig execute(AracnePreprocessInput input, String algorithm) throws RemoteException;
}
