package io.github.ppolushkin.newsAnalizer;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.auth.BatchSharedKeyCredentials;
import com.microsoft.azure.batch.protocol.models.*;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import org.joda.time.Period;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class AzureBatchStarter {

    public static void main(String argv[]) throws Exception {

        String batchAccount = "errbsrhgj4rykbatch";
        String batchUri = "https://errbsrhgj4rykbatch.francecentral.batch.azure.com";
        String batchKey = "u74DoJ+YynZeI59p/Cqe4bqSg/UsUcDqkOT1NgSX9s/KzAaStSxvk13/RH+4MeQqwIkH9MnUCqgxfiRNzvmHQA==";

        String storageAccountName = "errbsrhgj4rykstorage";
        String storageAccountKey = "06aoDLIDPkQ4Nb6fdoeiXldwrXjoeGlA9E1NrzAcLd0LLCE8UzOO1R5X24TCLAASqrUiymNISHBwDC3gTR9P6g==";

        Boolean shouldDeleteContainer = false;
        Boolean shouldDeleteJob = false;
        Boolean shouldDeletePool = true;

        Duration TASK_COMPLETE_TIMEOUT = Duration.ofMinutes(10);

        // Create batch client
        BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(batchUri, batchAccount, batchKey);
        BatchClient client = BatchClient.open(cred);

        // Create storage container
        CloudBlobContainer container = createBlobContainer(storageAccountName, storageAccountKey);

        String poolId = "newsAnalyzerPoolAuto";
        String jobId = "newsAnalyserJobAuto";
        try {
            CloudPool sharedPool = createPoolIfNotExists(client, poolId);
            submitJobAndAddTask(client, container, sharedPool.id(), jobId);
            if (waitForTasksToComplete(client, jobId, TASK_COMPLETE_TIMEOUT)) {
                // Get the task command output file
                System.out.println("All tasks completed.");
            } else {
                throw new TimeoutException("Task did not complete within the specified timeout");
            }
        } catch (BatchErrorException err) {
            printBatchException(err);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            // Clean up the resource if necessary
            if (shouldDeleteJob) {
                try {
                    client.jobOperations().deleteJob(jobId);
                } catch (BatchErrorException err) {
                    printBatchException(err);
                }
            }

            if (shouldDeletePool) {
                try {
                    client.poolOperations().deletePool(poolId);
                } catch (BatchErrorException err) {
                    printBatchException(err);
                }
            }

            if (shouldDeleteContainer) {
                container.deleteIfExists();
            }
        }
    }

    /**
     * Create IaaS pool if pool isn't exist
     *
     * @param client batch client instance
     * @param poolId the pool id
     * @return the pool instance
     * @throws BatchErrorException
     * @throws IllegalArgumentException
     * @throws IOException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private static CloudPool createPoolIfNotExists(BatchClient client, String poolId)
            throws BatchErrorException, IllegalArgumentException, IOException, InterruptedException, TimeoutException {
        // Create a pool with 1 A1 VM
        String osPublisher = "Canonical";
        String osOffer = "UbuntuServer";
        String poolVMSize = "STANDARD_A1";
        int poolVMCount = 0;
        Duration POOL_STEADY_TIMEOUT = Duration.ofMinutes(5);
        Duration VM_READY_TIMEOUT = Duration.ofMinutes(20);

        // Check if pool exists
        if (!client.poolOperations().existsPool(poolId)) {

            // See detail of creating IaaS pool at
            // https://blogs.technet.microsoft.com/windowshpc/2016/03/29/introducing-linux-support-on-azure-batch/
            // Get the sku image reference
            List<NodeAgentSku> skus = client.accountOperations().listNodeAgentSkus();
            String skuId = null;
            ImageReference imageRef = null;

            for (NodeAgentSku sku : skus) {
                if (sku.osType() == OSType.LINUX) {
                    for (ImageReference imgRef : sku.verifiedImageReferences()) {
                        if (imgRef.publisher().equalsIgnoreCase(osPublisher)
                                && imgRef.offer().equalsIgnoreCase(osOffer)) {
                            imageRef = imgRef;
                            skuId = sku.id();
                            break;
                        }
                    }
                }
            }

            // Use IaaS VM with Linux
            VirtualMachineConfiguration configuration = new VirtualMachineConfiguration();
            configuration.withNodeAgentSKUId(skuId).withImageReference(imageRef);

            client.poolOperations().createPool(poolId, poolVMSize, configuration, poolVMCount);
            StartTask installJava = new StartTask()
                    .withCommandLine("apt install openjdk-8-jre-headless -y")
                    .withWaitForSuccess(true)
                    .withUserIdentity(new UserIdentity()
                            .withAutoUser(new AutoUserSpecification()
                                    .withScope(AutoUserScope.POOL)
                                    .withElevationLevel(ElevationLevel.ADMIN)
                            ));
            ApplicationPackageReference packageReference = new ApplicationPackageReference().withApplicationId("newsAnalyzer").withVersion("1");
            client.poolOperations().patchPool(poolId, installJava, null, Arrays.asList(packageReference), null, null);


            String formula = "$samples = $ActiveTasks.GetSamplePercent(TimeInterval_Minute * 5);\n" +
                    "$tasks = $samples < 70 ? max(0, $ActiveTasks.GetSample(1)) :\n" +
                    "max( $ActiveTasks.GetSample(1), avg($ActiveTasks.GetSample(TimeInterval_Minute * 5)));\n" +
                    "$targetVMs = $tasks > 0 ? $tasks : max(0, $TargetDedicated / 2);\n" +
                    "cappedPoolSize = 2;\n" +
                    "$TargetDedicated = max(0, min($targetVMs, cappedPoolSize));\n" +
                    "$NodeDeallocationOption = taskcompletion;";

            client.poolOperations().enableAutoScale(poolId, formula, Period.minutes(5));
        }

        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;
        boolean steady = false;

        // Wait for the VM to be allocated
        while (elapsedTime < POOL_STEADY_TIMEOUT.toMillis()) {
            CloudPool pool = client.poolOperations().getPool(poolId);
            if (pool.allocationState() == AllocationState.STEADY) {
                steady = true;
                break;
            }
            System.out.println("wait 30 seconds for pool steady...");
            Thread.sleep(30 * 1000);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        if (!steady) {
            throw new TimeoutException("The pool did not reach a steady state in the allotted time");
        }

//        // The VMs in the pool don't need to be in and IDLE state in order to submit a
//        // job.
//        // The following code is just an example of how to poll for the VM state
//        startTime = System.currentTimeMillis();
//        elapsedTime = 0L;
//        boolean hasIdleVM = false;
//
//        // Wait for at least 1 VM to reach the IDLE state
//        while (elapsedTime < VM_READY_TIMEOUT.toMillis()) {
//            List<ComputeNode> nodeCollection = client.computeNodeOperations().listComputeNodes(poolId,
//                    new DetailLevel.Builder().withSelectClause("id, state").withFilterClause("state eq 'idle'")
//                            .build());
//            if (!nodeCollection.isEmpty()) {
//                hasIdleVM = true;
//                break;
//            }
//
//            System.out.println("wait 30 seconds for VM start...");
//            Thread.sleep(30 * 1000);
//            elapsedTime = (new Date()).getTime() - startTime;
//        }
//
//        if (!hasIdleVM) {
//            throw new TimeoutException("The node did not reach an IDLE state in the allotted time");
//        }

        return client.poolOperations().getPool(poolId);
    }

    /**
     * Create blob container in order to upload file
     *
     * @param storageAccountName storage account name
     * @param storageAccountKey  storage account key
     * @return CloudBlobContainer instance
     * @throws URISyntaxException
     * @throws StorageException
     */
    private static CloudBlobContainer createBlobContainer(String storageAccountName, String storageAccountKey)
            throws URISyntaxException, StorageException {
        String CONTAINER_NAME = "poolsandresourcefiles";

        // Create storage credential from name and key
        StorageCredentials credentials = new StorageCredentialsAccountAndKey(storageAccountName, storageAccountKey);

        // Create storage account. The 'true' sets the client to use HTTPS for
        // communication with the account
        CloudStorageAccount storageAccount = new CloudStorageAccount(credentials, true);

        // Create the blob client
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

        // Get a reference to a container.
        // The container name must be lower case
        return blobClient.getContainerReference(CONTAINER_NAME);
    }

    /**
     * Create a job with a single task
     *
     * @param client    batch client instance
     * @param container blob container to upload the resource file
     * @param poolId    pool id
     * @param jobId     job id
     * @throws BatchErrorException
     * @throws IOException
     */
    private static void submitJobAndAddTask(BatchClient client, CloudBlobContainer container, String poolId, String jobId) throws BatchErrorException, IOException {

        boolean jobExist = client.jobOperations().listJobs().stream().anyMatch(j -> j.id().equals(jobId));
        if (!jobExist) {
            // Create job run at the specified pool
            PoolInformation poolInfo = new PoolInformation();
            poolInfo.withPoolId(poolId);
            client.jobOperations().createJob(jobId, poolInfo);
        }

        LocalDate today = LocalDate.now();
        for (int day = 0; day < 30; day++) {

            LocalDate date = today.minusDays(day);
            String cmd = String.format("bash -c \"java -jar $AZ_BATCH_APP_PACKAGE_newsanalyzer_1/newsAnalyzer-1.jar --date=%s\"", date);

            final String taskId = "task2-" + date.toString();
            try {
                CloudTask task = client.taskOperations().getTask(jobId, taskId);
                if (task != null) {
                    continue;
                }
            } catch (BatchErrorException e) {
                //not 100% correct
                System.out.println("Task " + taskId + " is not created yet. Will create now.");
            }

            TaskAddParameter taskToAdd = new TaskAddParameter();
            taskToAdd.withId(taskId).withCommandLine(cmd);

//todo: when you know datastorage
//            OutputFile outputFile = new OutputFile();
//            outputFile.withFilePattern("**/*.cvs");
//            outputFile.withDestination();
//            taskToAdd.withOutputFiles(Arrays.asList(outputFile));

            // Add task to job
            client.taskOperations().createTask(jobId, taskToAdd);
        }

    }

    /**
     * Wait all tasks under a specified job to be completed
     *
     * @param client     batch client instance
     * @param jobId      job id
     * @param expiryTime the waiting period
     * @return if task completed in time, return true, otherwise, return false
     * @throws BatchErrorException
     * @throws IOException
     * @throws InterruptedException
     */
    private static boolean waitForTasksToComplete(BatchClient client, String jobId, Duration expiryTime)
            throws BatchErrorException, IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;

        while (elapsedTime < expiryTime.toMillis()) {
            List<CloudTask> taskCollection = client.taskOperations().listTasks(jobId,
                    new DetailLevel.Builder().withSelectClause("id, state").build());

            boolean allComplete = true;
            for (CloudTask task : taskCollection) {
                if (task.state() != TaskState.COMPLETED) {
                    allComplete = false;
                    break;
                }
            }

            if (allComplete) {
                // All tasks completed
                return true;
            }

            System.out.println("wait 10 seconds for tasks to complete...");

            // Check again after 10 seconds
            Thread.sleep(10 * 1000);
            elapsedTime = (new Date()).getTime() - startTime;
        }

        // Timeout, return false
        return false;
    }

    /**
     * print BatchErrorException to console
     *
     * @param err BatchErrorException instance
     */
    private static void printBatchException(BatchErrorException err) {
        System.out.println(String.format("BatchError %s", err.toString()));
        if (err.body() != null) {
            System.out.println(String.format("BatchError code = %s, message = %s", err.body().code(),
                    err.body().message().value()));
            if (err.body().values() != null) {
                for (BatchErrorDetail detail : err.body().values()) {
                    System.out.println(String.format("Detail %s=%s", detail.key(), detail.value()));
                }
            }
        }
    }

    /**
     * Upload file to blob container and return sas key
     *
     * @param container blob container
     * @param fileName  the file name of blob
     * @param filePath  the local file path
     * @return SAS key for the uploaded file
     * @throws URISyntaxException
     * @throws IOException
     * @throws InvalidKeyException
     * @throws StorageException
     */
    private static String uploadFileToCloud(CloudBlobContainer container, String fileName, String filePath)
            throws URISyntaxException, IOException, InvalidKeyException, StorageException {
        // Create the container if it does not exist.
        container.createIfNotExists();

        // Upload file
        CloudBlockBlob blob = container.getBlockBlobReference(fileName);
        File source = new File(filePath);
        blob.upload(new FileInputStream(source), source.length());

        // Create policy with 1 day read permission
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        EnumSet<SharedAccessBlobPermissions> perEnumSet = EnumSet.of(SharedAccessBlobPermissions.READ);
        policy.setPermissions(perEnumSet);

        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DATE, 1);
        policy.setSharedAccessExpiryTime(c.getTime());

        // Create SAS key
        String sas = blob.generateSharedAccessSignature(policy, null);
        return blob.getUri() + "?" + sas;
    }


}
