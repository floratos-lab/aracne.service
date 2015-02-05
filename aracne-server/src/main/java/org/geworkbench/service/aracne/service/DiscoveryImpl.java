package org.geworkbench.service.aracne.service;

import java.io.File;
import java.rmi.RemoteException;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geworkbench.service.aracne.schema.AracneInput;
import org.geworkbench.service.aracne.schema.AracneOutput;

public class DiscoveryImpl implements Discovery {
    private static final Log    log                 = LogFactory.getLog(DiscoveryImpl.class);
    
    private static final String targetFile          = "target.txt";
    private static final String configKernelFile    = "config_kernel.txt";
    private static final String configThresholdFile = "config_threshold.txt";
	private static final String aracneFile          = "aracne_submit.sh";
	private static final String consensusFile       = "consensus_submit.sh";
	private static final String configKernel        = "> bcell_mas5\n0.52477\t-0.24\n";
	private static final String configThreshold     = "> bcell_mas5\n1.062\t-48.7\t-0.634\n";
	private static final String adaptivePartitioning = "Adaptive Partitioning";

	@Override
	public AracneOutput execute(AracneInput input) throws RemoteException {
		String mode = input.getMode();
		if(mode.equals("Complete")) throw new RemoteException("'Complete' mode is no longer supported.");

		int nboot = input.getBootstrapNumber();
		
		// step 1: store files
		String runId = Utils.getRunId("ara");
		String aracneDir = Utils.ARACNE_RUNS_DIR+runId+"/";
    	if (runId==null) {
    		throw new RemoteException("Not able to create Aracne run directory on server:\n"+aracneDir);
    	}

		// discovery mode: use default or customized configuration
		String config_kernel = input.getConfigKernel();
		String config_threshold = input.getConfigThreshold();
		if (!input.getAlgorithm().equalsIgnoreCase(adaptivePartitioning)) {
			if (config_kernel == null || config_kernel.equals(""))
				config_kernel = configKernel;
			Utils.writeToFile(config_kernel, configKernelFile, aracneDir);
		}
		if (config_threshold == null || config_threshold.equals(""))
			config_threshold = configThreshold;
		Utils.writeToFile(config_threshold, configThresholdFile, aracneDir);

    	Utils.exportExp(input.getExpFile(), input.getDataSetName(), aracneDir);
    	Utils.writeToFile(input.getHubGeneList(), Utils.HUB_FILE, aracneDir);
    	Utils.writeToFile(input.getTargetGeneList(),    targetFile, aracneDir);
    	Utils.writeToFile(Utils.prepareAracne(input, runId),  aracneFile, aracneDir);
		if(input.getBootstrapNumber() > 1)
			Utils.writeToFile(Utils.prepareConsensus(input, runId), consensusFile, aracneDir);
		
        // step 2: compute
		AracneOutput output = new AracneOutput();
        output.setAdjName(runId);

		int ret = Utils.submitJob(aracneDir + aracneFile);
		log.info("SubmitJob aracne returns: "+ret);

		Utils.waitForJob(runId);

		String adjDir = aracneDir;
		if(nboot > 1) adjDir += Utils.ADJS_DIR;
		File adjFile = Utils.getAdjFile(adjDir);
		if(adjFile == null) return output;

		if(nboot > 1){
			ret = Utils.submitJob(aracneDir + consensusFile);
			log.info("SubmitJob consensus returns: "+ret);

			Utils.waitForJob(runId);

			adjFile = Utils.getAdjFile(aracneDir);
			if(adjFile == null) return output;
		}
		
        log.info("Sending aracne output " + runId);
        DataSource source = new FileDataSource(adjFile);
        output.setAdjFile(new DataHandler(source));

		return output;
	}

}
