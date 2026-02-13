package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "regexp"
    "sync"
    "time"

    "github.com/Azure/azure-sdk-for-go/sdk/azcore"
    "github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
    "github.com/Azure/azure-sdk-for-go/sdk/azidentity"
    "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4"
    "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computefleet/armcomputefleet/v2"
    "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computeschedule/armcomputeschedule"
    "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2"
    "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
    "github.com/google/uuid"
)

const (
    // NOTE: Replace with your subscription ID or set AZURE_SUBSCRIPTION_ID env var.
    defaultSubscriptionID = "31352ba3-4576-4d93-9214-7c2d18b24067"

    // Demo plan naming:
    //   BA-DEMO-{LANG}-SDK-RG (e.g., BA-DEMO-GO-SDK-RG)
    //   BA-DEMO-VN
    defaultResourceGroupName = "BA-DEMO-GO-SDK-RG"
    defaultVnetName          = "BA-DEMO-VN"

    location = "uksouth"
)

var (
    subscriptionID string
    resourceGroupName string
    vnetName string

    conn azcore.TokenCredential
    ctx  context.Context

    resourcesClientFactory       *armresources.ClientFactory
    networkClientFactory         *armnetwork.ClientFactory
    computeFleetClientFactory    *armcomputefleet.ClientFactory
    computeScheduleClientFactory *armcomputeschedule.ClientFactory
    computeClientFactory         *armcompute.ClientFactory

    resourceGroupClient    *armresources.ResourceGroupsClient
    providerClient         *armresources.ProvidersClient
    virtualNetworksClient  *armnetwork.VirtualNetworksClient
    fleetsClient           *armcomputefleet.FleetsClient
    scheduledActionsClient *armcomputeschedule.ScheduledActionsClient
    virtualMachinesClient  *armcompute.VirtualMachinesClient
)

func main() {
    // Set up the environment, create the clients, the resource group and virtual network
    setup()

    // 1) Create 1K VMs of Regular priority using VM Sizes
    baRegular1K := "BA-REGULAR-1K-VMs"
    createBulkActions(
        baRegular1K,
        /*capacityType*/ armcomputefleet.CapacityTypeVM,
        /*spotCapacity*/ 0,
        /*regularCapacity*/ 1000,
        /*vmSizesProfile*/ []*armcomputefleet.VMSizeProfile{
            {Name: to.Ptr("Standard_F1s")},
            {Name: to.Ptr("Standard_DS1_v2")},
            {Name: to.Ptr("Standard_D2ads_v5")},
            {Name: to.Ptr("Standard_D8as_v5")},
        },
        /*vmAttributes*/ nil,
    )

    // 2) GET the list of succeeded VMs from the BA using LIST VMs.
    //    LIST VMs can be used for any operationStatus (Creating/Succeeded/Failed/etc.).
    succeededStatus := armcomputefleet.VMOperationStatus("Succeeded")
    succeededVMs := listVMsInBulkAction(baRegular1K, &succeededStatus)
    log.Printf("Succeeded VMs in %s: %d", baRegular1K, len(succeededVMs))

    // 3) Register the subscription with the Microsoft.ComputeSchedule resource provider.
    //    This is required for ExecuteDelete APIs.
    _, err := providerClient.Register(ctx, "Microsoft.ComputeSchedule", nil)
    logIfError(err)

    // 4) DELETE failed VMs using ExecuteDelete API if any (force delete) + retries.
    //    ANY Azure VMs can be deleted through this API, not only those created by BulkActions.
    failedStatus := armcomputefleet.VMOperationStatus("Failed")
    failedVMs := listVMsInBulkAction(baRegular1K, &failedStatus)
    if len(failedVMs) > 0 {
        bulkDeleteVMsInBatch(failedVMs, /*forceDelete*/ true)
    } else {
        log.Printf("No failed VMs to delete for %s", baRegular1K)
    }

    // 5) Create 80K vCPUs of Spot priority using VM Attributes.
    baSpot80K := "BA-SPOT-ATTR-80K-vCPU"
    createBulkActions(
        baSpot80K,
        /*capacityType*/ armcomputefleet.CapacityTypeVCPU,
        /*spotCapacity*/ 80000,
        /*regularCapacity*/ 0,
        /*vmSizesProfile*/ nil,
        /*vmAttributes*/ saberCortexAttributes(),
    )

    // 6) After 5 mins, GET the list of VMs still in progress from the BA using LIST VMs.
    //    "Creating" indicates VMs in the process of being created/scheduled.
    log.Printf("Sleeping 5 minutes before checking progress for %s...", baSpot80K)
    time.Sleep(5 * time.Minute)

    creatingStatus := armcomputefleet.VMOperationStatus("Creating")
    creatingVMs := listVMsInBulkAction(baSpot80K, &creatingStatus)

    // 7) DELETE VMs still in progress using ExecuteDelete API if any (force delete).
    if len(creatingVMs) > 0 {
        bulkDeleteVMsInBatch(creatingVMs, /*forceDelete*/ true)
    } else {
        log.Printf("No in-progress (Creating) VMs to delete for %s", baSpot80K)
    }

    // 8) Create 4 concurrent BAs of 20K each.
    log.Printf("Creating 4 concurrent Spot BulkActions (20K vCPU each)...")
    var wg sync.WaitGroup
    for i := 1; i <= 4; i++ {
        wg.Add(1)
        go func(idx int) {
            defer wg.Done()
            name := fmt.Sprintf("BA-SPOT-ATTR-20K-vCPU-%d", idx)
            createBulkActions(
                name,
                /*capacityType*/ armcomputefleet.CapacityTypeVCPU,
                /*spotCapacity*/ 20000,
                /*regularCapacity*/ 0,
                /*vmSizesProfile*/ nil,
                /*vmAttributes*/ saberCortexAttributes(),
            )
        }(i)
    }
    wg.Wait()

    // 9) Delete resource group to cleanup.
    deleteResourceGroup()
}

func setup() {
    ctx = context.Background()

    subscriptionID = os.Getenv("AZURE_SUBSCRIPTION_ID")
    if subscriptionID == "" {
        subscriptionID = defaultSubscriptionID
    }

    resourceGroupName = os.Getenv("DEMO_RESOURCE_GROUP")
    if resourceGroupName == "" {
        resourceGroupName = defaultResourceGroupName
    }

    vnetName = os.Getenv("DEMO_VNET")
    if vnetName == "" {
        vnetName = defaultVnetName
    }

    conn = authenticate()

    var err error

    resourcesClientFactory, err = armresources.NewClientFactory(subscriptionID, conn, nil)
    logIfError(err)
    resourceGroupClient = resourcesClientFactory.NewResourceGroupsClient()
    providerClient = resourcesClientFactory.NewProvidersClient()

    networkClientFactory, err = armnetwork.NewClientFactory(subscriptionID, conn, nil)
    logIfError(err)
    virtualNetworksClient = networkClientFactory.NewVirtualNetworksClient()

    computeFleetClientFactory, err = armcomputefleet.NewClientFactory(subscriptionID, conn, nil)
    logIfError(err)
    fleetsClient = computeFleetClientFactory.NewFleetsClient()

    computeScheduleClientFactory, err = armcomputeschedule.NewClientFactory(subscriptionID, conn, nil)
    logIfError(err)
    scheduledActionsClient = computeScheduleClientFactory.NewScheduledActionsClient()

    computeClientFactory, err = armcompute.NewClientFactory(subscriptionID, conn, nil)
    logIfError(err)
    virtualMachinesClient = computeClientFactory.NewVirtualMachinesClient()

    createResourceGroup()
    createVirtualNetwork()
}

func createResourceGroup() {
    log.Printf("Creating resource group %s...", resourceGroupName)

    parameters := armresources.ResourceGroup{Location: to.Ptr(location)}
    _, err := resourceGroupClient.CreateOrUpdate(ctx, resourceGroupName, parameters, nil)
    logIfError(err)

    log.Printf("Created resource group: %s", resourceGroupName)
}

func deleteResourceGroup() {
    log.Printf("Deleting resource group %s...", resourceGroupName)

    poller, err := resourceGroupClient.BeginDelete(ctx, resourceGroupName, nil)
    logIfError(err)
    _, err = poller.PollUntilDone(ctx, nil)
    logIfError(err)

    log.Printf("Deleted resource group: %s", resourceGroupName)
}

func createVirtualNetwork() {
    log.Printf("Creating virtual network %s...", vnetName)

    parameters := armnetwork.VirtualNetwork{
        Location: to.Ptr(location),
        Properties: &armnetwork.VirtualNetworkPropertiesFormat{
            AddressSpace: &armnetwork.AddressSpace{
                AddressPrefixes: []*string{to.Ptr("10.1.0.0/16")},
            },
            Subnets: []*armnetwork.Subnet{
                {
                    Name: to.Ptr("default"),
                    Properties: &armnetwork.SubnetPropertiesFormat{
                        AddressPrefix: to.Ptr("10.1.0.0/18"),
                    },
                },
            },
        },
    }

    poller, err := virtualNetworksClient.BeginCreateOrUpdate(ctx, resourceGroupName, vnetName, parameters, nil)
    logIfError(err)
    _, err = poller.PollUntilDone(ctx, nil)
    logIfError(err)

    log.Printf("Created virtual network %s", vnetName)
}

func createBulkActions(
    bulkActionsName string,
    capacityType armcomputefleet.CapacityType,
    spotCapacity int32,
    regularCapacity int32,
    vmSizesProfile []*armcomputefleet.VMSizeProfile,
    vmAttributes *armcomputefleet.VMAttributes,
) {
    log.Printf("Creating BulkActions %s...", bulkActionsName)

    adminPassword := os.Getenv("DEMO_ADMIN_PASSWORD")
    if adminPassword == "" {
        adminPassword = "REPLACE_ME" // set DEMO_ADMIN_PASSWORD for an actual run
    }

    // Make the computer name prefix unique per BulkActions to reduce collision risk.
    prefix := safePrefix(bulkActionsName)

    parameters := armcomputefleet.Fleet{
        Location: to.Ptr(location),
        Properties: &armcomputefleet.FleetProperties{
            Mode:         to.Ptr(armcomputefleet.FleetModeInstance),
            CapacityType: to.Ptr(capacityType),
            SpotPriorityProfile: &armcomputefleet.SpotPriorityProfile{
                Capacity: to.Ptr(spotCapacity),
            },
            RegularPriorityProfile: &armcomputefleet.RegularPriorityProfile{
                Capacity: to.Ptr(regularCapacity),
            },
            VMSizesProfile: vmSizesProfile,
            VMAttributes:   vmAttributes,
            ComputeProfile: &armcomputefleet.ComputeProfile{
                BaseVirtualMachineProfile: &armcomputefleet.BaseVirtualMachineProfile{
                    StorageProfile: &armcomputefleet.VirtualMachineScaleSetStorageProfile{
                        ImageReference: &armcomputefleet.ImageReference{
                            Publisher: to.Ptr("Canonical"),
                            Offer:     to.Ptr("ubuntu-24_04-lts"),
                            SKU:       to.Ptr("server-gen1"),
                            Version:   to.Ptr("latest"),
                        },
                        OSDisk: &armcomputefleet.VirtualMachineScaleSetOSDisk{
                            OSType:       to.Ptr(armcomputefleet.OperatingSystemTypesLinux),
                            CreateOption: to.Ptr(armcomputefleet.DiskCreateOptionTypesFromImage),
                            DeleteOption: to.Ptr(armcomputefleet.DiskDeleteOptionTypesDelete),
                            Caching:      to.Ptr(armcomputefleet.CachingTypesReadWrite),
                            ManagedDisk: &armcomputefleet.VirtualMachineScaleSetManagedDiskParameters{
                                StorageAccountType: to.Ptr(armcomputefleet.StorageAccountTypesStandardLRS),
                            },
                        },
                    },
                    OSProfile: &armcomputefleet.VirtualMachineScaleSetOSProfile{
                        ComputerNamePrefix: to.Ptr(prefix),
                        AdminUsername:      to.Ptr("sample-user"),
                        AdminPassword:      to.Ptr(adminPassword),
                    },
                    NetworkProfile: &armcomputefleet.VirtualMachineScaleSetNetworkProfile{
                        NetworkAPIVersion: to.Ptr(armcomputefleet.NetworkAPIVersionV20201101),
                        NetworkInterfaceConfigurations: []*armcomputefleet.VirtualMachineScaleSetNetworkConfiguration{
                            {
                                Name: to.Ptr("nic"),
                                Properties: &armcomputefleet.VirtualMachineScaleSetNetworkConfigurationProperties{
                                    Primary:            to.Ptr(true),
                                    EnableIPForwarding: to.Ptr(true),
                                    IPConfigurations: []*armcomputefleet.VirtualMachineScaleSetIPConfiguration{
                                        {
                                            Name: to.Ptr("ip"),
                                            Properties: &armcomputefleet.VirtualMachineScaleSetIPConfigurationProperties{
                                                Subnet: &armcomputefleet.APIEntityReference{
                                                    ID: to.Ptr("/subscriptions/" + subscriptionID + "/resourceGroups/" + resourceGroupName + "/providers/Microsoft.Network/virtualNetworks/" + vnetName + "/subnets/default"),
                                                },
                                                Primary: to.Ptr(true),
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }

    poller, err := fleetsClient.BeginCreateOrUpdate(ctx, resourceGroupName, bulkActionsName, parameters, nil)
    logIfError(err)

    _, err = poller.PollUntilDone(ctx, nil)
    logIfError(err)

    log.Printf("Created BulkActions %s", bulkActionsName)
}

func listVMsInBulkAction(bulkActionsName string, operationStatus *armcomputefleet.VMOperationStatus) []*string {
    var allVMs []*string

    var options *armcomputefleet.FleetsClientListVirtualMachinesOptions
    if operationStatus != nil && string(*operationStatus) != "" {
        // OData filter. Example: operationStatus eq 'Succeeded'
        options = &armcomputefleet.FleetsClientListVirtualMachinesOptions{
            Filter: to.Ptr("operationStatus eq '" + string(*operationStatus) + "'"),
        }
    }

    pager := fleetsClient.NewListVirtualMachinesPager(resourceGroupName, bulkActionsName, options)
    for pager.More() {
        page, err := pager.NextPage(ctx)
        logIfError(err)
        for _, v := range page.Value {
            allVMs = append(allVMs, v.ID)
        }
    }

    log.Printf("Total VMs found in BulkActions %s (status=%v): %d", bulkActionsName, operationStatus, len(allVMs))
    return allVMs
}

func bulkDeleteVMsInBatch(vmIDs []*string, forceDelete bool) {
    if len(vmIDs) == 0 {
        log.Printf("No VMs to delete")
        return
    }

    var wg sync.WaitGroup
    for _, b := range batch(vmIDs, 100) {
        wg.Add(1)
        go func(batchIDs []*string) {
            defer wg.Done()
            bulkDeleteVMs(batchIDs, forceDelete)
        }(b)
    }
    wg.Wait()
}

func bulkDeleteVMs(vmIDs []*string, forceDelete bool) {
    log.Printf("Starting ExecuteDelete for %d VMs (force=%v)", len(vmIDs), forceDelete)

    corr := uuid.New().String()

    // Start deletion (retries with a 15-minute window).
    // NOTE: This API can delete ANY Azure VMs given their ARM IDs.
    deleteResp, err := scheduledActionsClient.VirtualMachinesExecuteDelete(ctx, location, armcomputeschedule.ExecuteDeleteRequest{
        ExecutionParameters: &armcomputeschedule.ExecutionParameters{
            RetryPolicy: &armcomputeschedule.RetryPolicy{
                RetryCount:          to.Ptr[int32](5),
                RetryWindowInMinutes: to.Ptr[int32](15),
            },
        },
        Resources:     &armcomputeschedule.Resources{IDs: vmIDs},
        Correlationid: to.Ptr(corr),
        ForceDeletion: to.Ptr(forceDelete),
    }, nil)
    logIfError(err)

    // Poll until terminal state (Succeeded/Failed/Canceled).
    pollCtx, cancel := context.WithTimeout(ctx, 60*time.Minute)
    defer cancel()

    opsReq := getOpsRequest(deleteResp, corr)

    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-pollCtx.Done():
            log.Fatalf("Timed out polling delete operations (corr=%s)", corr)
        case <-ticker.C:
            statusResp, err := scheduledActionsClient.VirtualMachinesGetOperationStatus(pollCtx, location, opsReq, nil)
            logIfError(err)

            done, failedCount := isPollingComplete(statusResp)
            if done {
                if failedCount > 0 {
                    log.Printf("ExecuteDelete completed with %d failed operations (corr=%s)", failedCount, corr)
                } else {
                    log.Printf("ExecuteDelete completed successfully (corr=%s)", corr)
                }
                return
            }
        }
    }
}

func getOpsRequest(
    deleteResponse armcomputeschedule.ScheduledActionsClientVirtualMachinesExecuteDeleteResponse,
    corr string,
) armcomputeschedule.GetOperationStatusRequest {
    var operationIDs []*string

    for _, result := range deleteResponse.Results {
        if result.Operation != nil && result.Operation.OperationID != nil {
            operationIDs = append(operationIDs, result.Operation.OperationID)
        }
    }

    return armcomputeschedule.GetOperationStatusRequest{
        OperationIDs:  operationIDs,
        Correlationid: to.Ptr(corr),
    }
}

func isPollingComplete(getOpsResponse armcomputeschedule.ScheduledActionsClientVirtualMachinesGetOperationStatusResponse) (done bool, failedCount int) {
    for _, result := range getOpsResponse.Results {
        op := result.Operation
        if op == nil || op.State == nil {
            return false, failedCount
        }

        state := string(*op.State)
        switch state {
        case "Succeeded":
            // ok
        case "Failed", "Canceled", "Cancelled":
            failedCount++
        default:
            // InProgress, Queued, etc.
            return false, failedCount
        }
    }
    return true, failedCount
}

func authenticate() azcore.TokenCredential {
    cred, err := azidentity.NewDefaultAzureCredential(nil)
    logIfError(err)
    return cred
}

func logIfError(err error) {
    if err != nil {
        log.Fatal(err)
    }
}

func batch[T any](input []T, batchSize int) [][]T {
    var batches [][]T

    for i := 0; i < len(input); i += batchSize {
        end := i + batchSize
        if end > len(input) {
            end = len(input)
        }
        batches = append(batches, input[i:end])
    }

    return batches
}

func saberCortexAttributes() *armcomputefleet.VMAttributes {
    return &armcomputefleet.VMAttributes{
        VCPUCount: &armcomputefleet.VMAttributeMinMaxInteger{
            Min: to.Ptr[int32](1),
            Max: to.Ptr[int32](64),
        },
        MemoryInGiB: &armcomputefleet.VMAttributeMinMaxDouble{
            Min: to.Ptr(8.0),
            Max: to.Ptr(256.0),
        },
        MemoryInGiBPerVCpu: &armcomputefleet.VMAttributeMinMaxDouble{
            Min: to.Ptr(8.0),
            Max: to.Ptr(8.0),
        },
    }
}

func safePrefix(name string) string {
    // Keep alphanumerics and dash, lowercased.
    re := regexp.MustCompile(`[^a-zA-Z0-9-]+`)
    cleaned := re.ReplaceAllString(name, "")
    if cleaned == "" {
        cleaned = "ba"
    }

    // Compute name prefix length limit varies by image; keep it small.
    cleaned = cleaned
    if len(cleaned) > 10 {
        cleaned = cleaned[:10]
    }

    // Add a short random suffix to avoid collisions.
    suffix := uuid.New().String()[:4]
    return fmt.Sprintf("%s-%s", cleaned, suffix)
}
