package org.lab41.dendrite.services.analysis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusPipeline;
import com.thinkaurelius.faunus.formats.edgelist.EdgeListOutputFormat;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.tinkerpop.blueprints.Vertex;

import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.lab41.dendrite.jobs.FaunusJob;
import org.lab41.dendrite.metagraph.DendriteGraph;
import org.lab41.dendrite.metagraph.models.JobMetadata;
import org.lab41.dendrite.services.MetaGraphService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class SnapCommunity extends AnalysisService {

    Logger logger = LoggerFactory.getLogger(SnapCommunity.class);
    private org.apache.commons.configuration.Configuration config;

    private static List<String> algorithms = Arrays.asList(
        "bigclam"
    );

    @Autowired
    ResourceLoader resourceLoader;

    @Autowired
    MetaGraphService metaGraphService;

    @Autowired
    FaunusPipelineService faunusPipelineService;

    @Value("${snap.properties}")
    String pathToProperties;

    @Async
    public void snapCommunityDetection(DendriteGraph graph, String algorithm, String jobId) throws Exception {
        try {
            if (!algorithms.contains(algorithm)) {
                throw new Exception("invalid algorithm selected");
            }

            org.springframework.core.io.Resource resource = resourceLoader.getResource(pathToProperties);
            config = new PropertiesConfiguration(resource.getFile());

            logger.debug("Starting Snap "
                    + algorithm + " analysis on "
                    + graph.getId()
                    + " job " + jobId
                    + " " + Thread.currentThread().getName());

            setJobName(jobId, "snap_"+algorithm);
            setJobState(jobId, JobMetadata.RUNNING);

            run(graph, jobId, algorithm);
        } catch (Exception e) {
            logger.debug("snap-" + algorithm + ": error: ", e);
            e.printStackTrace();
            setJobState(jobId, JobMetadata.ERROR, e.getMessage());
            throw e;
        }

        setJobState(jobId, JobMetadata.DONE);

        logger.debug("Snap " + algorithm + ": finished job: " + jobId);
    }

    Configuration conf = new YarnConfiguration();
  
    public void run(DendriteGraph graph, String jobId, String algorithm) throws Exception {
        logger.debug("starting snap community detection analysis of '" + graph.getId() + "'");

        FileSystem fs = FileSystem.get(new Configuration());

        // Create the temporary directories.
        Path tmpDir = new Path(
                new Path(new Path(fs.getHomeDirectory(), "dendrite"), "tmp"),
                UUID.randomUUID().toString());

        Path shellPath = new Path("args[0]");
        shellPath = FileSystem.get(conf).makeQualified(shellPath);
        Path snapPath = new Path("args[1]");
        snapPath = FileSystem.get(conf).makeQualified(snapPath);
        final int n = Integer.valueOf("args[2]");
        Path jarPath = new Path("args[3]");
        jarPath = FileSystem.get(conf).makeQualified(jarPath);

        // Create yarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
    
        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = 
            Records.newRecord(ContainerLaunchContext.class);
        String cmd = "/bin/sh snap.sh";

        amContainer.setCommands(
            Collections.singletonList(
                "$JAVA_HOME/bin/java" +
                " -Xmx256M" +
                " org.lab41.yarnsnap.ApplicationMaster" +
                " " + String.valueOf(n) +
                " " + cmd +
                " snap.sh " + shellPath +
                " snapfunc " + snapPath +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr" 
                )
            );
    
        // Setup jar for ApplicationMaster
        Map<String, LocalResource> localResources =
            new HashMap<String, LocalResource>();
    
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(jarPath, appMasterJar);
        localResources.put("yarnsnap.jar", appMasterJar);

        amContainer.setLocalResources(localResources);

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);
    
        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = 
        app.getApplicationSubmissionContext();
        appContext.setApplicationName("yarnsnap"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue 

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);
    
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED && 
               appState != YarnApplicationState.KILLED && 
               appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }
    
        System.out.println(
            "Application " + appId + " finished with" +
            " state " + appState + 
            " at " + appReport.getFinishTime());

    }
  
    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                c.trim());
        }
        Apps.addToEnvironment(appMasterEnv,
            Environment.CLASSPATH.name(),
            Environment.PWD.$() + File.separator + "*");
    }
}
