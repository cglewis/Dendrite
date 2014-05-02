package org.lab41.dendrite.services.analysis;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class YarnApplicationMaster {

  Configuration conf = new YarnConfiguration();

  private void setupSnapCommand(Path snapPath, LocalResource snapCommand) throws IOException {
    FileStatus snapStat = FileSystem.get(conf).getFileStatus(snapPath);
    snapCommand.setResource(ConverterUtils.getYarnUrlFromPath(snapPath));
    snapCommand.setSize(snapStat.getLen());
    snapCommand.setTimestamp(snapStat.getModificationTime());
    snapCommand.setType(LocalResourceType.FILE);
    snapCommand.setVisibility(LocalResourceVisibility.PUBLIC);
  }

  private void setupShellScript(Path shellPath, LocalResource shellScript) throws IOException {
    FileStatus shellStat = FileSystem.get(conf).getFileStatus(shellPath);
    shellScript.setResource(ConverterUtils.getYarnUrlFromPath(shellPath));
    shellScript.setSize(shellStat.getLen());
    shellScript.setTimestamp(shellStat.getModificationTime());
    shellScript.setType(LocalResourceType.FILE);
    shellScript.setVisibility(LocalResourceVisibility.PUBLIC);
  }

  public void run(String[] args) throws Exception {
    final String command = args[1] + " " + args[2];
    final int n = Integer.valueOf(args[0]);
    final String shellName = args[3];
    Path shellPath = new Path(args[4]);
    shellPath = FileSystem.get(conf).makeQualified(shellPath);
    final String snapName = args[5];
    Path snapPath = new Path(args[6]);
    snapPath = FileSystem.get(conf).makeQualified(snapPath);
    
    // Initialize clients to ResourceManager and NodeManagers
    Configuration conf = new YarnConfiguration();

    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    System.out.println("registerApplicationMaster 0");
    rmClient.registerApplicationMaster("", 0, "");
    System.out.println("registerApplicationMaster 1");
    
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(128);
    capability.setVirtualCores(1);

    // Make container requests to ResourceManager
    for (int i = 0; i < n; ++i) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
      System.out.println("Making res-req " + i);
      rmClient.addContainerRequest(containerAsk);
    }

    // Obtain allocated containers and launch 
    int allocatedContainers = 0;
    int completedContainers = 0;
    while (allocatedContainers < n) {
      AllocateResponse response = rmClient.allocate(0);
      for (Container container : response.getAllocatedContainers()) {
        ++allocatedContainers;

        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = 
            Records.newRecord(ContainerLaunchContext.class);

        Map<String, LocalResource> localResources =
            new HashMap<String, LocalResource>();

        LocalResource shellScript = Records.newRecord(LocalResource.class);
        setupShellScript(shellPath, shellScript);
        localResources.put(shellName, shellScript);

        LocalResource snapCommand = Records.newRecord(LocalResource.class);
        setupSnapCommand(snapPath, snapCommand);
        localResources.put(snapName, snapCommand);

        ctx.setLocalResources(localResources);

        System.out.println("Local resources: " + ctx.getLocalResources());

        ctx.setCommands(
            Collections.singletonList(
                command + 
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr" 
                ));

        System.out.println("Launching container " + allocatedContainers);
        nmClient.startContainer(container, ctx);
      }
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        ++completedContainers;
        System.out.println("Completed container " + completedContainers);
      }
      Thread.sleep(100);
    }

    // Now wait for containers to complete
    while (completedContainers < n) {
      AllocateResponse response = rmClient.allocate(completedContainers/n);
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        ++completedContainers;
        System.out.println("Completed container " + completedContainers);
      }
      Thread.sleep(100);
    }

    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(
        FinalApplicationStatus.SUCCEEDED, "", "");
  }

  public static void main(String[] args) throws Exception {
    YarnApplicationMaster am = new YarnApplicationMaster();
    am.run(args);
  }

}
