package org.geworkbench.service.aracne.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import javax.activation.DataHandler;

import org.geworkbench.service.aracne.schema.AracnePreprocessInput;
import org.geworkbench.service.aracne.service.StubAracneInputRepository.StreamGobbler;

/* Collection of the static methods used by the other classes in this package. */
public class Utils {

	private static final String SGE_CLUSTER_NAME = "hpc";
	private static final String SGE_ROOT = "/opt/gridengine/"+SGE_CLUSTER_NAME;
	
	private static final String USER_HOME = "/ifs/data/c2b2/af_lab/cagrid/";
	private static final String ARACNE_RUNS_DIR = USER_HOME+"r/aracne/runs/";
	private static final String ARACNEBIN           = USER_HOME+"r/aracne/bin/";

    private static final Random random              = new Random();

    private static final String configLog           = "config.log";
	private static final String configMat           = "running_config.m";
	private static final String kernelFunc          = "generate_kernel_width_configuration";
	private static final String thresholdFunc       = "generate_mutual_threshold_configuration";
    private static final long   POLL_INTERVAL       = 20000; //20 seconds
	private static final String maxmem              = "6G";
	private static final String timeout             = "48::";

	public static String getRunId(String code){
		File root = new File(ARACNE_RUNS_DIR);
		if (!root.exists() && !root.mkdir()) return null;

		int i = 0;
		String runid = null;
		File randdir = null;
		do {
			runid = code + random.nextInt(Short.MAX_VALUE);
			randdir = new File(ARACNE_RUNS_DIR + runid + "/");
		} while (randdir.exists() && ++i < Short.MAX_VALUE);
		
		if (i < Short.MAX_VALUE && randdir.mkdir())
			return runid;
		return null;
	}

	public static void exportExp(DataHandler handler, String fname, String dir) throws RemoteException{
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

	public static String prepareConfigSh(String runId){
		StringBuilder builder = new StringBuilder();
		builder.append("#!/bin/bash\n#$ -l mem="+maxmem+",time="+timeout)
		.append(" -cwd -j y -o ").append(ARACNE_RUNS_DIR+runId+"/"+configLog).append(" -N ").append(runId)
		.append("\ncd ").append(ARACNE_RUNS_DIR+runId)
		.append("\n\n/nfs/apps/matlab/2012a/bin/matlab -nodisplay -nodesktop -nosplash < ")
		.append(configMat);

		return builder.toString();
	}
	
	public static String prepareConfigMat(AracnePreprocessInput input){
		StringBuilder builder = new StringBuilder("clc\nclear\n");
	    builder.append("src_dir = '").append(ARACNEBIN).append("';\n")
		.append("addpath(src_dir)\n")
		.append("filename_exp = '").append(input.getDataSetName()).append("';\n")
		.append("method = '").append(input.getAlgorithm().toLowerCase().replaceAll(" ", "_")).append("';\n")
		.append("data = importdata(filename_exp);\n");	 
	    if (input.getAlgorithm().toLowerCase().contains("fixed bandwidth"))	    	
	    	builder.append(kernelFunc).append("(data.data);\n");
		builder.append(thresholdFunc).append("(data.data, method);\n");    
		return builder.toString();
	}

	public static void writeToFile(String string, String fname, String dir) throws RemoteException{
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
	
	public static int submitJob(java.lang.String jobfile) throws RemoteException{
		String[] command = {SGE_ROOT+"/bin/lx-amd64/qsub", jobfile};
		System.out.println(command);
		try {
			ProcessBuilder pb = new ProcessBuilder(command);
			Map<String, String> env = pb.environment();
			env.put("SGE_ROOT", SGE_ROOT);
			env.put("SGE_CLUSTER_NAME", SGE_CLUSTER_NAME);
			env.put("PATH", SGE_ROOT+"/bin/lx-amd64:$PATH");
			Process p = pb.start();
			StreamGobbler out = new StreamGobbler(p.getInputStream(), "INPUT");
			StreamGobbler err = new StreamGobbler(p.getErrorStream(), "ERROR");
			out.start();
			err.start();
			return p.waitFor();

		} catch (Exception e) {
			throw new RemoteException("Aracne submitJob Exception", e);
		}
	}

	public static void waitForJob(String runId) throws RemoteException{
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

	public static boolean isJobDone(String runid) throws RemoteException {
		String cmd = SGE_ROOT+"/bin/lx-amd64/qstat";
		BufferedReader brIn = null;
		BufferedReader brErr = null;
		try{
			ProcessBuilder pb = new ProcessBuilder(cmd);
			Map<String, String> env = pb.environment();
			env.put("SGE_ROOT", SGE_ROOT);
			env.put("SGE_CLUSTER_NAME", SGE_CLUSTER_NAME);
			env.put("PATH", SGE_ROOT+"/bin/lx-amd64:$PATH");
			Process p = pb.start();
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

	public static String runError(String logfname){
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

	public static ArrayList<Float> getConfig(File file){
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
	
}