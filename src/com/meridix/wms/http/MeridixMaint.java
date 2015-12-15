package com.meridix.wms.http;

import java.io.*;
import java.net.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Date;

//import com.meridix.wms.module.EventInfo;

//import com.wowza.wms.application.Application;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.http.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.stream.MediaStreamMap;
import com.wowza.wms.vhost.*;

public class MeridixMaint extends HTTProvider2Base {

 
	private String meridixStoragePath = "/mnt/s3/";
	private String tempStoragePath = "/home/wowza/content/";
	private String completedStoragePath = tempStoragePath + "completed/";
	private String formatExt = ".mp4";
	private String meridixManagerAPIUrl = "https://api.meridix.com/liveid/manager/manager.php";
	private String nodeURL = "http://localhost:8809/";
	private IApplicationInstance appInstance;
	
	public void onHTTPRequest(IVHost vhost, IHTTPRequest req, IHTTPResponse resp) {
		if (!doHTTPAuthentication(vhost, req, resp))
			return;

		String statusStr = "OK";
		String actionStr = "";
		String eventIdStr = "";
		String appStr = "live";
		String errorStr = "";
		
		StringBuffer ret = new StringBuffer();
		ret.append("<?xml version=\"1.0\"?>\n<WowzaMediaServer>");
		
		
		if (req.getMethod().equalsIgnoreCase("post")) {
			req.parseBodyForParams(true);
		}

		Map<String, List<String>> params = req.getParameterMap();

		if (params.containsKey("app")){
			appStr = params.get("app").get(0);
		}

		if (false == vhost.applicationExists(appStr)){
			statusStr = "Error";
			errorStr = "Invalid Application";
			getLogger().info("DBGVH:: " + statusStr + " " + errorStr + " " + appStr);
		}
		if (statusStr != "Error" && vhost.isApplicationLoaded(appStr) == false){
			// attempt to start the application
			vhost.startApplicationInstance(appStr);
		}
		
		if (statusStr != "Error" && vhost.isApplicationLoaded(appStr)){
			// appInstance = vhost.getApplication(appStr).getAppInstance("_definst_");
			appInstance = vhost.getApplication(appStr).getAppInstance("_definst_");
						
			meridixStoragePath = appInstance.getProperties().getPropertyStr("meridixStoragePath");
			meridixManagerAPIUrl = appInstance.getProperties().getPropertyStr(
					"meridixManagerAPIUrl", meridixManagerAPIUrl);

			
			tempStoragePath = appInstance.getStreamStorageDir();
			
			String completedStorageFolder = appInstance.getProperties().getPropertyStr("meridixCompletedStorageFolder");
			completedStoragePath = appInstance.getStreamStorageDir() + File.separator + completedStorageFolder + File.separator; 

			if (!(tempStoragePath.endsWith("\\") || tempStoragePath.endsWith("/"))) {
				tempStoragePath += File.separatorChar;
			}
			if (!(meridixStoragePath.endsWith("\\") || meridixStoragePath.endsWith("/"))) {
				meridixStoragePath += File.separatorChar;
			}
			if (!(completedStoragePath.endsWith("\\") || completedStoragePath.endsWith("/"))) {
				completedStoragePath += File.separatorChar;
			}
			
		}else {
			statusStr = "Error";
			errorStr = "Application " + appStr + " not loaded";
			getLogger().info("DBGVH:: " + statusStr + " " + errorStr);
		}
		
		
		ret.append("<app>" + appStr + "</app>");
		
		if(params.containsKey("action")){
			actionStr = params.get("action").get(0);
		}else {
			statusStr = "Error";
			errorStr = "Missing action";
			getLogger().info("DBGVH:: " + statusStr + " " + errorStr);
		}
		
		if (statusStr == "OK"){
			if (! actionStr.equalsIgnoreCase("archive") 
					&& ! actionStr.equalsIgnoreCase("delete") 
					&& ! actionStr.equalsIgnoreCase("resume") 
					&& ! actionStr.equalsIgnoreCase("getLiveStreamNames") 
					&& ! actionStr.contentEquals("isrecording")){
				statusStr = "Error";
				errorStr = "Invalid action: " + actionStr;
				getLogger().info("DBGVH:: " + statusStr + " " + errorStr);

			}else{
				ret.append("<action>" + actionStr + "</action>");
			}
		}

		// Check for a valid eventId. We skip this check if we are 
		// getting live stream names
		//
		if (! actionStr.equalsIgnoreCase("getLiveStreamNames")){
			if (statusStr == "OK")
				if(params.containsKey("eventid")){
					eventIdStr = params.get("eventid").get(0);
					ret.append("<eventid>" + eventIdStr + "</eventid>");
				}else {
					statusStr = "Error";
					errorStr = "Missing eventid";
					getLogger().info("DBGVH:: " + statusStr + " " + errorStr);
				}
			
			if (statusStr == "OK"){
				if(eventIdStr == ""){
					statusStr = "Error";
					errorStr = "Invalid eventid";
					getLogger().info("DBGVH:: " + statusStr + " " + errorStr);
				}			
			}
			
		}
				
		if (statusStr == "OK" && "resume".equalsIgnoreCase(actionStr)){
			//File resumeFile = new File(completedStoragePath + eventIdStr + formatExt);
			File resumeFile = new File(completedStoragePath + eventIdStr );
			if (! resumeFile.exists()){
				statusStr = "Error";
				errorStr = "No file to resume";
				getLogger().info("DBGVH:: " + statusStr + " " + errorStr + " " + eventIdStr);
			}
		}
		ret.append("<status>" + statusStr + "</status>");		
		
		if (statusStr == "OK"){

			if (actionStr.contentEquals("isrecording")){
				if (isEventRecording( appStr, eventIdStr)) {
					ret.append("<is_recording>1</is_recording>");
				}else{
					ret.append("<is_recording>0</is_recording>");					
				}
			} 
			else if (actionStr.contentEquals("getLiveStreamNames")){
			
				MediaStreamMap streams = appInstance.getStreams();
				List<String> streamNames = streams.getPublishStreamNames();
				
				Iterator<String> iter = streamNames.iterator();
				ret.append("<liveStreams>");
				while(iter.hasNext()){
					String streamName = iter.next();
					ret.append("<stream>");
					ret.append("<name>" + streamName + "</name>");
					ret.append("<eventid>" + "01234" + "</eventid>");
					ret.append("<is_recording>" + 0 + "</is_recording>");					
					ret.append("</stream>");
				}				
				ret.append("</liveStreams>");				
			}			
			else {
				processRequest(appStr, actionStr, eventIdStr);				
			}
			
		
		}else{
			ret.append("<error_reason>" + errorStr + "</error_reason>");
		}
		
		Date now = new Date();
		
		ret.append("<time>" + now.toString()+ "</time>" );
		ret.append("</WowzaMediaServer>");
		
		try {
			resp.setHeader("Content-Type", "text/xml");
			
			OutputStream out = resp.getOutputStream();
			byte[] outBytes = ret.toString().getBytes();
			out.write(outBytes);
		} catch (Exception e) {
			WMSLoggerFactory.getLogger(null).error(
					"DBGVH:: ERROR MeridixArchiveMaint: " + e.toString());
		}

	}
	
	private void processRequest(String appstr, String action, String eventid) {
		
		String myURL = nodeURL;
										
		try {
			// We now only attempt to copy the archive file if it is marked as "A" (archive).
			// That prevents us from holding the file open if a new publish/append event comes in.
			//
			if ("archive".equalsIgnoreCase(action)){
				myURL += "onunpublish/" + eventid;
			}
			else if("delete".equalsIgnoreCase(action)){
				myURL += "ondelete/" + eventid;	
			}
			else if("resume".equalsIgnoreCase(action)){
				myURL += "onresume/" + eventid;
			}
			
			URL node = new URL(myURL);
			
			//URLConnection tnCon = node.openConnection();
			InputStreamReader is = new InputStreamReader(node.openStream());
			BufferedReader in = new BufferedReader(is);
			try {
				while(in.readLine() != null){};
			}
			finally {
				in.close();
			}				
					
		}
		catch (IOException e) {
			getLogger().error(e);
		}
	}



	private boolean isEventRecording(String appstr, String eventid) {

//		String recordFileName = tempStoragePath + eventid + formatExt + ".tmp";					
//		
//		
//		File recordedFile = new File(recordFileName);
//		
//		if (recordedFile.exists()){
//			return true;
//		}
//
//		return false;

		
		
		String recorders = appInstance.getProperties().getPropertyStr("recorders");
		
		getLogger().info("DBGVH:: List of current recordings: " + recorders);
		
		if ( recorders.length() > 0 && recorders.indexOf("," + eventid) >= 0 )
		{
			return true;
		}	
		else {
			return false;
		}
		
	}

	private WMSLogger getLogger(){
		
		return WMSLoggerFactory.getLogger(null);
	}
	
}