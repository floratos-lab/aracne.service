package org.geworkbench.service.aracne.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Random;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geworkbench.service.aracne.schema.AracneConfig;
import org.geworkbench.service.aracne.schema.AracneInput;
import org.geworkbench.service.aracne.schema.AracneOutput;


public class StubAracneInputRepository implements AracneInputRepository {
    private static final Log    log                 = LogFactory.getLog(StubAracneInputRepository.class);
	private static final String ARACNEROOT          = "/ifs/data/c2b2/af_lab/cagrid/r/aracne/runs/";
	private static final String ARACNEBIN           = "/ifs/data/c2b2/af_lab/cagrid/r/aracne/bin/";
	private static final String adjsDir             = "adjfiles";
	private static final String logsDir             = "logs";
    private static final String aracneLog           = "aracne.log";
    private static final String consensusLog        = "consensus.log";
    private static final String configLog           = "config.log";
    private static final String hubFile             = "hub.txt";
    private static final String targetFile          = "target.txt";
    private static final String configKernelFile    = "config_kernel.txt";
    private static final String configThresholdFile = "config_threshold.txt";
	private static final String aracneFile          = "aracne_submit.sh";
	private static final String consensusFile       = "consensus_submit.sh";
	private static final String configSh            = "config_submit.sh";
	private static final String configMat           = "running_config.m";
	private static final String kernelFunc          = "generate_kernel_width_configuration";
	private static final String thresholdFunc       = "generate_mutual_threshold_configuration";
	private static final String aracneBinName       = "aracne2";
	private static final String consensusBinName    = "getconsensusnet.pl";
    private static final Random random              = new Random();
    private static final long   POLL_INTERVAL       = 20000; //20 seconds
	private static final String maxmem              = "6G";
	private static final String timeout             = "48::";
	private static final String configKernel        = "> bcell_mas5\n0.52477\t-0.24\n";
	private static final String configThreshold     = "> bcell_mas5\n1.062\t-48.7\t-0.634\n";

	@Override
	public String storeAracneInput(AracneInput input) throws RemoteException{
		String runId = getRunId("ara");
		String aracneDir = ARACNEROOT+runId+"/";
    	if (runId==null) {
    		throw new RemoteException("Not able to create Aracne run directory on server:\n"+aracneDir);
    	}

    	if(input.getMode().equals("Complete")){
    		//complete mode: generate configuration
    		writeToFile(prepareConfigSh(runId),     configSh, aracneDir);
        	writeToFile(prepareConfigMat(input),   configMat, aracneDir);
    	}else{
    		//discovery mode: use default or customized configuration
    		String config_kernel    = input.getConfigKernel();
    		String config_threshold = input.getConfigThreshold();
    		if(config_kernel == null || config_kernel.equals(""))       config_kernel = configKernel;
    		if(config_threshold == null || config_threshold.equals("")) config_threshold = configThreshold;
	    	writeToFile(config_kernel,      configKernelFile, aracneDir);
	    	writeToFile(config_threshold,configThresholdFile, aracneDir);
    	}
    	
    	exportExp(input.getExpFile(), input.getDataSetName(), aracneDir);
    	writeToFile(input.getHubGeneList(),          hubFile, aracneDir);
    	writeToFile(input.getTargetGeneList(),    targetFile, aracneDir);
    	writeToFile(prepareAracne(input, runId),  aracneFile, aracneDir);
		if(input.getBootstrapNumber() > 1)
			writeToFile(prepareConsensus(input, runId), consensusFile, aracneDir);

		return runId;
	}
	
	@Override
	public String storeConfigInput(AracneInput input) throws RemoteException{
		String runId = getRunId("cfg");
		String aracneDir = ARACNEROOT+runId+"/";
    	if (runId==null) {
    		throw new RemoteException("Not able to create Aracne run directory on server:\n"+aracneDir);
    	}
    	
    	exportExp(input.getExpFile(), input.getDataSetName(), aracneDir);
    	writeToFile(prepareConfigSh(runId),  configSh, aracneDir);
    	writeToFile(prepareConfigMat(input), configMat, aracneDir);
    	return runId;
	}

	private String getRunId(String code){
		File root = new File(ARACNEROOT);
		if (!root.exists() && !root.mkdir()) return null;

		int i = 0;
		String runid = null;
		File randdir = null;
		do {
			runid = code + random.nextInt(Short.MAX_VALUE);
			randdir = new File(ARACNEROOT + runid + "/");
		} while (randdir.exists() && ++i < Short.MAX_VALUE);
		
		if (i < Short.MAX_VALUE && randdir.mkdir())
			return runid;
		return null;
	}
	
	private void exportExp(DataHandler handler, String fname, String dir) throws RemoteException{
		if(handler == null) return;
    	File expfile = new File(dir+fname);
    	OutputStream os = null;
    	try{
    		os = new FileOutputStream(expfile);
        	handler.writeTo(os);
    	}catch(IOException e){
    		e.printStackTrace();
    		throw new RemoteException("Aracne exportExp Exception", e);
    	}finally{
    		if(os != null){
				try{ os.close(); }catch(Exception e){ e.printStackTrace(); }
			}
    	}
	}
	
	private void writeToFile(String string, String fname, String dir) throws RemoteException{
		if(string == null || string.length() == 0) return;
	    BufferedWriter bw = null;
	    try{
			bw = new BufferedWriter(new FileWriter(dir+fname));
			bw.write(string);
			bw.flush();
	    }catch(IOException e){
	    	e.printStackTrace();
	    	throw new RemoteException("Aracne writeToFile Exception", e);
	    }finally{
	    	if(bw != null){
				try{ bw.close(); }catch(Exception e){ e.printStackTrace(); }
			}
	    }
	}

	private String prepareAracne(AracneInput input, String runId){
		int nboot = input.getBootstrapNumber();
		if(nboot>1){
			new File(ARACNEROOT+runId+"/"+logsDir).mkdir();
			new File(ARACNEROOT+runId+"/"+adjsDir).mkdir();
		}
		
		StringBuilder builder = new StringBuilder();
		builder.append("#!/bin/bash\n#$ -l mem="+maxmem+",time="+timeout);
		if(nboot > 1) builder.append(" -t 1-").append(nboot);
		builder.append(" -cwd -j y -o ").append(ARACNEROOT+runId+"/"+aracneLog).append(" -N ").append(runId)
		.append("\nexport JOBNAME=\"").append(runId)
		.append("\"\nexport INFILE=\"").append(input.getDataSetName())
		.append("\"\nexport HUBFILE=\"").append(hubFile)
		.append("\"\nexport BINDIR=\"").append(ARACNEBIN)
		.append("\"\nexport JOBDIR=\"").append(ARACNEROOT).append("$JOBNAME")
		.append("\"\nexport LOGS=\"").append(logsDir)
		.append("\"\nexport ADJ=\"").append(adjsDir)
		.append("\"\n\ncd \"$JOBDIR\"\n")
		.append("\n\"$BINDIR\"/").append(aracneBinName)
		.append(" -i \"$INFILE\"")
		.append(" -a ").append(input.getAlgorithm().replaceAll(" ", "_"));
		if(nboot > 1) builder.append(" -r \"$SGE_TASK_ID\"");
		if(input.isIsKernelWidthSpecified())
			builder.append(" -k ").append(input.getKernelWidth());
		if(input.isIsDPIToleranceSpecified())
			builder.append(" -e ").append(input.getDPITolerance());
		builder.append(input.isIsThresholdMI()?" -t ":" -p ").append(input.getThreshold())
		.append(" -H \"$JOBDIR\"  -s \"$HUBFILE\" -l \"$HUBFILE\"");
		if(nboot > 1) 
			builder.append(" -o \"$ADJ/$INFILE\"_r\"$SGE_TASK_ID\".adj >& \"$LOGS/$JOBNAME\"_\"$SGE_TASK_ID\".out");
		else builder.append(" >& \"$JOBNAME\".out");
		
		return builder.toString();
	}
	
	private String prepareConsensus(AracneInput input, String runId){
		StringBuilder builder = new StringBuilder();
		builder.append("#!/bin/bash\n#$ -l mem="+maxmem+",time="+timeout);
		builder.append(" -cwd -j y -o ").append(ARACNEROOT+runId+"/"+consensusLog).append(" -N ").append(runId)
		.append("\nexport JOBNAME=\"").append(runId)
		.append("\"\nexport BINDIR=\"").append(ARACNEBIN)
		.append("\"\nexport JOBDIR=\"").append(ARACNEROOT).append("$JOBNAME")
		.append("\"\nexport LOGS=\"").append(logsDir)
		.append("\"\nexport ADJ=\"").append(adjsDir)
		.append("\"\n\ncd \"$JOBDIR\"\n")
		.append("\nperl \"$BINDIR\"/").append(consensusBinName).append(" \"$ADJ\" ")
		.append(input.getConsensusThreshold());
		//.append("\nrm -rf \"$ADJ\" \"$LOGS\"");
		
		return builder.toString();
	}

	private String prepareConfigSh(String runId){
		StringBuilder builder = new StringBuilder();
		builder.append("#!/bin/bash\n#$ -l mem="+maxmem+",time="+timeout)
		.append(" -cwd -j y -o ").append(ARACNEROOT+runId+"/"+configLog).append(" -N ").append(runId)
		.append("\ncd ").append(ARACNEROOT+runId)
		.append("\n\n/nfs/apps/matlab/2012a/bin/matlab -nodisplay -nodesktop -nosplash < ")
		.append(configMat);

		return builder.toString();
	}
	
	private String prepareConfigMat(AracneInput input){
		StringBuilder builder = new StringBuilder("clc\nclear\n");
	    builder.append("src_dir = '").append(ARACNEBIN).append("';\n")
		.append("addpath(src_dir)\n")
		.append("filename_exp = '").append(input.getDataSetName()).append("';\n")
		.append("method = '").append(input.getAlgorithm().toLowerCase().replaceAll(" ", "_")).append("';\n")
		.append("data = importdata(filename_exp);\n")
		.append(kernelFunc).append("(data.data);\n")
		.append(thresholdFunc).append("(data.data, method);\n");
		return builder.toString();
	}
	
	@Override
	public AracneOutput execute(String runId, int nboot, String mode) throws RemoteException{
		if(mode.equals("Complete")) executeConfig(runId);

		AracneOutput output = new AracneOutput();
        output.setAdjName(runId);

		String aracneDir = ARACNEROOT + runId + "/";
		int ret = submitJob(aracneDir + aracneFile);
		log.info("SubmitJob aracne returns: "+ret);

		waitForJob(runId);

		String adjDir = aracneDir;
		if(nboot > 1) adjDir += adjsDir;
		File adjFile = getAdjFile(adjDir);
		if(adjFile == null) return output;

		if(nboot > 1){
			ret = submitJob(aracneDir + consensusFile);
			log.info("SubmitJob consensus returns: "+ret);

			waitForJob(runId);

			adjFile = getAdjFile(aracneDir);
			if(adjFile == null) return output;
		}
		
        log.info("Sending aracne output " + runId);
        DataSource source = new FileDataSource(adjFile);
        output.setAdjFile(new DataHandler(source));

		return output;
	}
	
	@Override
	public AracneConfig executeConfig(String runId) throws RemoteException{
		String aracneDir = ARACNEROOT + runId + "/";
		int ret = submitJob(aracneDir + configSh);
		log.info("SubmitJob aracne config returns: "+ret);

		waitForJob(runId);
		
		File kernelFile = new File(aracneDir + configKernelFile);
		File thresholdFile = new File(aracneDir + configThresholdFile);
		if (!thresholdFile.exists()){
		    String err = null;
		    if ((err = runError(aracneDir + configLog)) != null)
		    	throw new RemoteException("Aracne config run for "+runId+" got error:\n"+err);
		    else
		    	throw new RemoteException("Aracne config run for "+runId+" was killed unexpectedly");
		}

		AracneConfig config = new AracneConfig();
		config.setName(runId);
		config.getKernel().addAll(getConfig(kernelFile));
		config.getThreshold().addAll(getConfig(thresholdFile));

		return config;
	}
		
	private void waitForJob(String runId) throws RemoteException{
		try{
	    	Thread.sleep(POLL_INTERVAL*3); //wait for a minute before polling results
	    }catch(InterruptedException e){
	    }

		while(!isJobDone(runId)){
		    try{
		    	Thread.sleep(POLL_INTERVAL);
		    }catch(InterruptedException e){
		    }
		}
	}
		
	private File getAdjFile(String adjDir){
		File resultDir = new File(adjDir);
		if (!resultDir.isDirectory()) return null;
		for(File file : resultDir.listFiles()){
			String fname = file.getName();
			if(fname.endsWith(".adj")) return file;
		}
		return null;
	}
	
	private ArrayList<Float> getConfig(File file){
		ArrayList<Float> list = new ArrayList<Float>();
		if(!file.exists()) return list;
		BufferedReader br = null;
		try{
			br = new BufferedReader(new FileReader(file));
			String line = null;
			while((line = br.readLine()) != null){
				if (line.trim().length() == 0 || line.startsWith(">"))
					continue;
				for(String tok : line.split("\t")){
					list.add(Float.parseFloat(tok));
				}
			}
		}catch(Exception e){
			e.printStackTrace();
			list.clear();
		}finally{
			try{
				if (br!=null) br.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return list;
	}

	private int submitJob(java.lang.String jobfile) throws RemoteException{
		String command = "qsub " + jobfile;
		System.out.println(command);
		try {
			Process p = Runtime.getRuntime().exec(command);
			StreamGobbler out = new StreamGobbler(p.getInputStream(), "INPUT");
			StreamGobbler err = new StreamGobbler(p.getErrorStream(), "ERROR");
			out.start();
			err.start();
			return p.waitFor();

		} catch (Exception e) {
			throw new RemoteException("Aracne submitJob Exception", e);
		}
	}
	
	private boolean isJobDone(String runid) throws RemoteException {
		String cmd = "qstat";
		BufferedReader brIn = null;
		BufferedReader brErr = null;
		try{
			Process p = Runtime.getRuntime().exec(cmd);
			brIn = new BufferedReader(new InputStreamReader(p.getInputStream()));
			brErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line = null;
			while ((line = brIn.readLine())!=null || (line = brErr.readLine())!=null){
				if(line.startsWith("error")) return false; //cluster scheduler error
				String[] toks = line.trim().split("\\s+");
				if (toks.length > 3 && toks[2].equals(runid))
					return false;
			}
		}catch(Exception e){
			throw new RemoteException("Aracne isJobDone Exception", e);
		}finally {
			try{
				if (brIn!=null)  brIn.close();
				if (brErr!=null) brErr.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return true;
	}

	private String runError(String logfname){
		StringBuilder str = new StringBuilder();
		BufferedReader br = null;
		boolean error = false;
		File logFile = new File(logfname);
		if (!logFile.exists()) return null;
		try{
			br = new BufferedReader(new FileReader(logFile));
			String line = null;
			while((line = br.readLine())!=null){
				if (line.contains("error")||line.contains("Error")){
					str.append(line + "\n");
					error = true;
				}
			}
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			try{
				if (br!=null) br.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		if (error)  return str.toString();
		return null;
	}

	public static class StreamGobbler extends Thread
	{
	    private InputStream is;
	    private String type;
	    private OutputStream os;
	    
	    StreamGobbler(InputStream is, String type)
	    {
	        this(is, type, null);
	    }
	    StreamGobbler(InputStream is, String type, OutputStream redirect)
	    {
	        this.is = is;
	        this.type = type;
	        this.os = redirect;
	    }
	    
	    public void run()
	    {
            PrintWriter pw = null;
            BufferedReader br = null;
	        try {
	            if (os != null)
	                pw = new PrintWriter(os, true);
	                
	            InputStreamReader isr = new InputStreamReader(is);
	            br = new BufferedReader(isr);
	            String line=null;
	            while ( (line = br.readLine()) != null)
	            {
	                if (pw != null){
	                    pw.println(line);
	                }
	                System.out.println(type + ">" + line);    
	            }
	        } catch (IOException ioe) {
	            ioe.printStackTrace();  
	        } finally {
	        	try{
		        	if (pw!=null) pw.close();
	        		if (br!=null) br.close();
	            }catch(Exception e){
	            	e.printStackTrace();
	            }
	        }
	    }
	}
}
