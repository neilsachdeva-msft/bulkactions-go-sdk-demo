package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v4"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computebulkactions/armcomputebulkactions"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/computeschedule/armcomputeschedule"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/google/uuid"
)

const (
	// NOTE: Replace with your subscription ID or set AZURE_SUBSCRIPTION_ID env var.
	defaultSubscriptionID = "1d04e8f1-ee04-4056-b0b2-718f5bb45b04"

	// Demo plan naming:
	//   BA-DEMO-{LANG}-SDK-RG (e.g., BA-DEMO-GO-SDK-RG)
	//   BA-DEMO-VN
	defaultResourceGroupName = "BA-DEMO-GO-SDK-RG"
	defaultVnetName          = "BA-DEMO-VN"

	location = "uksouth"
)

var (
	subscriptionID    string
	resourceGroupName string
	vnetName          string

	conn azcore.TokenCredential
	ctx  context.Context

	resourcesClientFactory       *armresources.ClientFactory
	networkClientFactory         *armnetwork.ClientFactory
	bulkActionsClientFactory     *armcomputebulkactions.ClientFactory
	computeScheduleClientFactory *armcomputeschedule.ClientFactory
	computeClientFactory         *armcompute.ClientFactory

	resourceGroupClient    *armresources.ResourceGroupsClient
	virtualNetworksClient  *armnetwork.VirtualNetworksClient
	bulkActionsClient      *armcomputebulkactions.BulkActionsClient
	scheduledActionsClient *armcomputeschedule.ScheduledActionsClient
	virtualMachinesClient  *armcompute.VirtualMachinesClient
)

func main() {
	// Set up the environment, create the clients, the resource group and virtual network
	setup()

	// 1) Create 1K VMs of Regular priority using VM Sizes.
	baRegular1KOpID := createBulkActions(
		armcomputebulkactions.CapacityTypeVM,
		1000,
		armcomputebulkactions.VirtualMachineTypeRegular,
		[]*armcomputebulkactions.VMSizeProfile{
			{Name: to.Ptr("Standard_F1s")},
			{Name: to.Ptr("Standard_DS1_v2")},
			{Name: to.Ptr("Standard_D2ads_v5")},
			{Name: to.Ptr("Standard_D8as_v5")},
		},
		nil,
	)

	// 2) GET the list of succeeded VMs from the BA using LIST VMs.
	//    LIST VMs can be used for any operationStatus (Creating/Succeeded/Failed/etc.).
	succeededStatus := armcomputebulkactions.VMOperationStatusSucceeded
	succeededVMs := listVMsInBulkAction(baRegular1KOpID, &succeededStatus)
	log.Printf("Succeeded VMs in %s: %d", baRegular1KOpID, len(succeededVMs))

	// 3) DELETE succeeded VMs using ExecuteDelete API (force delete) + retries.
	//    ANY Azure VMs can be deleted through this API, not only those created by BulkActions.
	if len(succeededVMs) > 0 {
		bulkDeleteVMsInBatch(succeededVMs /*forceDelete*/, true)
	} else {
		log.Printf("No succeeded VMs to delete for %s", baRegular1KOpID)
	}

	// 4) Create 40K vCPUs of Spot priority using VM Attributes.
	baSpot80KOpID := createBulkActions(
		armcomputebulkactions.CapacityTypeVCPU,
		40000,
		armcomputebulkactions.VirtualMachineTypeSpot,
		nil,
		vmAttributes(),
	)

	// 5) After 5 mins, GET the list of VMs still in progress from the BA using LIST VMs.
	//    "Creating" indicates VMs in the process of being created/scheduled.
	log.Printf("Sleeping 5 minutes before checking progress for %s...", baSpot80KOpID)
	time.Sleep(5 * time.Minute)

	creatingStatus := armcomputebulkactions.VMOperationStatusCreating
	creatingVMs := listVMsInBulkAction(baSpot80KOpID, &creatingStatus)

	// 6) DELETE VMs still in progress using ExecuteDelete API if any (force delete).
	if len(creatingVMs) > 0 {
		bulkDeleteVMsInBatch(creatingVMs /*forceDelete*/, true)
	} else {
		log.Printf("No in-progress (Creating) VMs to delete for %s", baSpot80KOpID)
	}

	// 7) Create 4 concurrent BAs of 10K each.
	log.Printf("Creating 4 concurrent Spot BulkActions (10K vCPU each)...")
	var wg sync.WaitGroup
	for i := 1; i <= 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_ = createBulkActions(
				armcomputebulkactions.CapacityTypeVCPU,
				10000,
				armcomputebulkactions.VirtualMachineTypeSpot,
				nil,
				vmAttributes(),
			)
		}(i)
	}
	wg.Wait()

	// 8) Delete resource group to cleanup.
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

	opts := clientOpts()

	resourcesClientFactory, err = armresources.NewClientFactory(subscriptionID, conn, opts)
	logIfError(err)
	resourceGroupClient = resourcesClientFactory.NewResourceGroupsClient()

	networkClientFactory, err = armnetwork.NewClientFactory(subscriptionID, conn, opts)
	logIfError(err)
	virtualNetworksClient = networkClientFactory.NewVirtualNetworksClient()

	bulkActionsClientFactory, err = armcomputebulkactions.NewClientFactory(subscriptionID, conn, opts)
	logIfError(err)
	bulkActionsClient = bulkActionsClientFactory.NewBulkActionsClient()

	computeScheduleClientFactory, err = armcomputeschedule.NewClientFactory(subscriptionID, conn, opts)
	logIfError(err)
	scheduledActionsClient = computeScheduleClientFactory.NewScheduledActionsClient()

	computeClientFactory, err = armcompute.NewClientFactory(subscriptionID, conn, opts)
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
	capacityType armcomputebulkactions.CapacityType,
	capacity int32,
	priorityType armcomputebulkactions.VirtualMachineType,
	vmSizesProfile []*armcomputebulkactions.VMSizeProfile,
	vmAttrs *armcomputebulkactions.VMAttributes,
) string {
	operationID := uuid.New().String()
	log.Printf("Creating BulkActions (operationID=%s)...", operationID)

	adminPassword := os.Getenv("DEMO_ADMIN_PASSWORD")
	if adminPassword == "" {
		adminPassword = "TestP@ssw0rd" // set DEMO_ADMIN_PASSWORD for an actual run
	}

	// Make the computer name unique per BulkActions to reduce collision risk.
	prefix := safePrefix(operationID)

	parameters := armcomputebulkactions.LocationBasedLaunchBulkInstancesOperation{
		Properties: &armcomputebulkactions.LaunchBulkInstancesOperationProperties{
			Capacity:     to.Ptr(capacity),
			CapacityType: to.Ptr(capacityType),
			PriorityProfile: &armcomputebulkactions.PriorityProfile{
				Type: to.Ptr(priorityType),
			},
			VMSizesProfile: vmSizesProfile,
			VMAttributes:   vmAttrs,
			ComputeProfile: &armcomputebulkactions.ComputeProfile{
				VirtualMachineProfile: &armcomputebulkactions.VirtualMachineProfile{
					StorageProfile: &armcomputebulkactions.StorageProfile{
						ImageReference: &armcomputebulkactions.ImageReference{
							Publisher: to.Ptr("Canonical"),
							Offer:     to.Ptr("ubuntu-24_04-lts"),
							SKU:       to.Ptr("server-gen1"),
							Version:   to.Ptr("latest"),
						},
						OSDisk: &armcomputebulkactions.OSDisk{
							OSType:       to.Ptr(armcomputebulkactions.OperatingSystemTypesLinux),
							CreateOption: to.Ptr(armcomputebulkactions.DiskCreateOptionTypesFromImage),
							DeleteOption: to.Ptr(armcomputebulkactions.DiskDeleteOptionTypesDelete),
							Caching:      to.Ptr(armcomputebulkactions.CachingTypesReadWrite),
							ManagedDisk: &armcomputebulkactions.ManagedDiskParameters{
								StorageAccountType: to.Ptr(armcomputebulkactions.StorageAccountTypesStandardLRS),
							},
						},
					},
					OSProfile: &armcomputebulkactions.OSProfile{
						ComputerName:  to.Ptr(prefix),
						AdminUsername: to.Ptr("sample-user"),
						AdminPassword: to.Ptr(adminPassword),
					},
					NetworkProfile: &armcomputebulkactions.NetworkProfile{
						NetworkAPIVersion: to.Ptr(armcomputebulkactions.NetworkAPIVersion20201101),
						NetworkInterfaceConfigurations: []*armcomputebulkactions.VirtualMachineNetworkInterfaceConfiguration{
							{
								Name: to.Ptr("nic"),
								Properties: &armcomputebulkactions.VirtualMachineNetworkInterfaceConfigurationProperties{
									Primary:            to.Ptr(true),
									EnableIPForwarding: to.Ptr(true),
									IPConfigurations: []*armcomputebulkactions.VirtualMachineNetworkInterfaceIPConfiguration{
										{
											Name: to.Ptr("ip"),
											Properties: &armcomputebulkactions.VirtualMachineNetworkInterfaceIPConfigurationProperties{
												Subnet: &armcomputebulkactions.SubResource{
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

	poller, err := bulkActionsClient.BeginCreateOrUpdate(ctx, resourceGroupName, location, operationID, parameters, nil)
	logIfError(err)

	_, err = poller.PollUntilDone(ctx, nil)
	logIfError(err)

	log.Printf("Created BulkActions (operationID=%s)", operationID)
	return operationID
}

func listVMsInBulkAction(operationID string, operationStatus *armcomputebulkactions.VMOperationStatus) []*string {
	var allVMs []*string

	// Fetch all VMs (no server-side $filter) and filter client-side,
	// because the API does not support filtering by OperationStatus.
	pager := bulkActionsClient.NewListVirtualMachinesPager(resourceGroupName, location, operationID, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		logIfError(err)
		for _, v := range page.Value {
			// If a status filter was requested, only include matching VMs.
			if operationStatus != nil {
				if v.OperationStatus == nil || *v.OperationStatus != *operationStatus {
					continue
				}
			}
			allVMs = append(allVMs, v.ID)
		}
	}

	statusStr := "<all>"
	if operationStatus != nil {
		statusStr = string(*operationStatus)
	}
	log.Printf("Total VMs found in BulkActions %s (status=%s): %d", operationID, statusStr, len(allVMs))
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
				RetryCount:           to.Ptr[int32](5),
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

// loggingPolicy logs HTTP errors only to reduce console noise.
type loggingPolicy struct{}

func (p *loggingPolicy) Do(req *policy.Request) (*http.Response, error) {
	resp, err := req.Next()
	if err != nil {
		raw := req.Raw()
		log.Printf("[HTTP] ERROR %s %s: %v", raw.Method, raw.URL.String(), err)
		return resp, err
	}

	if resp.StatusCode >= 400 {
		raw := req.Raw()
		corrID := resp.Header.Get("x-ms-correlation-request-id")
		log.Printf("[HTTP] %d %s %s | Correlation-ID: %s", resp.StatusCode, raw.Method, raw.URL.String(), corrID)

		if resp.Body != nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil {
				log.Printf("[HTTP] Response Body: %s", string(body))
				resp.Body = io.NopCloser(bytes.NewReader(body))
			}
		}
	}

	return resp, nil
}

func clientOpts() *arm.ClientOptions {
	return &arm.ClientOptions{
		ClientOptions: policy.ClientOptions{
			PerCallPolicies: []policy.Policy{&loggingPolicy{}},
			Cloud: cloud.Configuration{
				ActiveDirectoryAuthorityHost: "https://login.microsoftonline.com/",
				Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
					cloud.ResourceManager: {
						Endpoint: "https://uksouth.management.azure.com",
						Audience: "https://management.azure.com",
					},
				},
			},
		},
	}
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

func vmAttributes() *armcomputebulkactions.VMAttributes {
	return &armcomputebulkactions.VMAttributes{
		VCPUCount: &armcomputebulkactions.VMAttributeMinMaxInteger{
			Min: to.Ptr[int32](64),
			Max: to.Ptr[int32](320),
		},
		MemoryInGiB: &armcomputebulkactions.VMAttributeMinMaxDouble{
			Min: to.Ptr(100.0),
			Max: to.Ptr(3000.0),
		},
		MemoryInGiBPerVCpu: &armcomputebulkactions.VMAttributeMinMaxDouble{
			Min: to.Ptr(8.0),
		},
		ArchitectureTypes: []*armcomputebulkactions.ArchitectureType{
			to.Ptr(armcomputebulkactions.ArchitectureTypeX64),
		},
		CPUManufacturers: []*armcomputebulkactions.CPUManufacturer{
			to.Ptr(armcomputebulkactions.CPUManufacturerIntel),
		},
		ExcludedVMSizes: []*string{
			to.Ptr("Standard_L64s_v3"),
			to.Ptr("Standard_L80s_v3"),
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
	if len(cleaned) > 10 {
		cleaned = cleaned[:10]
	}

	// Add a short random suffix to avoid collisions.
	suffix := uuid.New().String()[:4]
	return fmt.Sprintf("%s-%s", cleaned, suffix)
}
