package org.geworkbench.service.aracne.service;

import java.io.File;
import java.rmi.RemoteException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geworkbench.service.aracne.schema.AracneConfig;
import org.geworkbench.service.aracne.schema.AracnePreprocessInput;

/* Aracne preprocessing should be completely separated from Aracne computation.
 * There is no sensible reason that they were implemented under a common interface. */
public class PreprocessImpl implements Preprocess {
	private static final Log    log                 = LogFactory.getLog(PreprocessImpl.class);
	
	private static final String USER_HOME = "/ifs/data/c2b2/af_lab/cagrid/";
	private static final String ARACNE_RUNS_DIR = USER_HOME+"r/aracne/runs/";

	private static final String configKernelFile    = "config_kernel.txt";
    private static final String configThresholdFile = "config_threshold.txt";
    private static final String configLog           = "config.log";
	private static final String configSh            = "config_submit.sh";
	private static final String configMat           = "running_config.m";
	private static final String adaptivePartitioning = "Adaptive Partitioning";

	@Override
	public
    AracneConfig execute(AracnePreprocessInput input, String algorithm) throws RemoteException {
		// step 1: store input
		String runId = Utils.getRunId("cfg");
		String aracneDir = ARACNE_RUNS_DIR+runId+"/";
    	if (runId==null) {
    		throw new RemoteException("Not able to create Aracne run directory on server:\n"+aracneDir);
    	}
    	
    	Utils.exportExp(input.getExpFile(), input.getDataSetName(), aracneDir);
    	Utils.writeToFile(Utils.prepareConfigSh(runId),  configSh, aracneDir);
    	Utils.writeToFile(Utils.prepareConfigMat(input), configMat, aracneDir);

    	// step 2: execute
		int ret = Utils.submitJob(aracneDir + configSh);
		log.info("SubmitJob aracne config returns: "+ret);

		Utils.waitForJob(runId);
		
		File kernelFile = new File(aracneDir + configKernelFile);
		File thresholdFile = new File(aracneDir + configThresholdFile);
		if (!thresholdFile.exists()){
		    String err = null;
		    if ((err = Utils.runError(aracneDir + configLog)) != null)
		    	throw new RemoteException("Aracne config run for "+runId+" got error:\n"+err);
		    else
		    	throw new RemoteException("Aracne config run for "+runId+" was killed unexpectedly");
		}

		AracneConfig config = new AracneConfig();
		if (algorithm.equalsIgnoreCase(adaptivePartitioning)) {
		     config.setName("AP - " + runId);
		} else {
			config.setName("FB - " + runId);
			config.getKernel().addAll(Utils.getConfig(kernelFile));
		}
		
		config.getThreshold().addAll(Utils.getConfig(thresholdFile));

		return config;
	}
	
}
