# BulkActions Demo (Go)

This project demonstrates the Azure Compute BulkActions Go SDK (`armcomputebulkactions`).

## Demo Flow

1. Create resource group **BA-DEMO-GO-SDK-RG** (override with `DEMO_RESOURCE_GROUP`).
2. Create virtual network **BA-DEMO-VN** (override with `DEMO_VNET`).
3. Create **1K Regular** VMs using VM sizes.
4. LIST succeeded VMs by `operationStatus`.
5. ExecuteDelete **succeeded** VMs with `forceDeletion=true` and retry window of 15 minutes.
6. Create **80K Spot vCPUs** using VM attributes (vCPU 64–320, Intel, X64, excluded sizes: L64s_v3/L80s_v3).
7. After 5 minutes, LIST VMs in `Creating` and delete them (if any) using ExecuteDelete.
8. Create **4 concurrent** Spot BulkActions of **20K vCPUs** each.
9. Delete resource group.

Region: **UKSouth**.

## Prerequisites
- Go installed
- `az login` or environment-based auth for `DefaultAzureCredential`

## Environment variables
- `AZURE_SUBSCRIPTION_ID`
- `DEMO_RESOURCE_GROUP` (optional)
- `DEMO_VNET` (optional)
- `DEMO_ADMIN_PASSWORD` (required for a real run)

## Run
```bash
export AZURE_SUBSCRIPTION_ID=<sub>
export DEMO_ADMIN_PASSWORD='<password>'
go run .
```
