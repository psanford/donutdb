package donutdb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (db *DonutDB) BatchExecuteStatement(*dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) BatchExecuteStatementWithContext(context.Context, *dynamodb.BatchExecuteStatementInput, ...request.Option) (*dynamodb.BatchExecuteStatementOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) BatchGetItem(*dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) BatchGetItemWithContext(context.Context, *dynamodb.BatchGetItemInput, ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) BatchGetItemPages(*dynamodb.BatchGetItemInput, func(*dynamodb.BatchGetItemOutput, bool) bool) error {
	return ToBeImplementedErr
}
func (db *DonutDB) BatchGetItemPagesWithContext(context.Context, *dynamodb.BatchGetItemInput, func(*dynamodb.BatchGetItemOutput, bool) bool, ...request.Option) error {
	return ToBeImplementedErr
}

func (db *DonutDB) BatchWriteItem(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) BatchWriteItemWithContext(context.Context, *dynamodb.BatchWriteItemInput, ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) CreateBackup(*dynamodb.CreateBackupInput) (*dynamodb.CreateBackupOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) CreateBackupWithContext(context.Context, *dynamodb.CreateBackupInput, ...request.Option) (*dynamodb.CreateBackupOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) CreateGlobalTable(*dynamodb.CreateGlobalTableInput) (*dynamodb.CreateGlobalTableOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) CreateGlobalTableWithContext(context.Context, *dynamodb.CreateGlobalTableInput, ...request.Option) (*dynamodb.CreateGlobalTableOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DeleteBackup(*dynamodb.DeleteBackupInput) (*dynamodb.DeleteBackupOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DeleteBackupWithContext(context.Context, *dynamodb.DeleteBackupInput, ...request.Option) (*dynamodb.DeleteBackupOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DeleteItemWithContext(context.Context, *dynamodb.DeleteItemInput, ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DeleteTable(*dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DeleteTableWithContext(context.Context, *dynamodb.DeleteTableInput, ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeBackup(*dynamodb.DescribeBackupInput) (*dynamodb.DescribeBackupOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeBackupWithContext(context.Context, *dynamodb.DescribeBackupInput, ...request.Option) (*dynamodb.DescribeBackupOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeContinuousBackups(*dynamodb.DescribeContinuousBackupsInput) (*dynamodb.DescribeContinuousBackupsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeContinuousBackupsWithContext(context.Context, *dynamodb.DescribeContinuousBackupsInput, ...request.Option) (*dynamodb.DescribeContinuousBackupsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeContributorInsights(*dynamodb.DescribeContributorInsightsInput) (*dynamodb.DescribeContributorInsightsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeContributorInsightsWithContext(context.Context, *dynamodb.DescribeContributorInsightsInput, ...request.Option) (*dynamodb.DescribeContributorInsightsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeEndpoints(*dynamodb.DescribeEndpointsInput) (*dynamodb.DescribeEndpointsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeEndpointsWithContext(context.Context, *dynamodb.DescribeEndpointsInput, ...request.Option) (*dynamodb.DescribeEndpointsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeExport(*dynamodb.DescribeExportInput) (*dynamodb.DescribeExportOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeExportWithContext(context.Context, *dynamodb.DescribeExportInput, ...request.Option) (*dynamodb.DescribeExportOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeGlobalTable(*dynamodb.DescribeGlobalTableInput) (*dynamodb.DescribeGlobalTableOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeGlobalTableWithContext(context.Context, *dynamodb.DescribeGlobalTableInput, ...request.Option) (*dynamodb.DescribeGlobalTableOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeGlobalTableSettings(*dynamodb.DescribeGlobalTableSettingsInput) (*dynamodb.DescribeGlobalTableSettingsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeGlobalTableSettingsWithContext(context.Context, *dynamodb.DescribeGlobalTableSettingsInput, ...request.Option) (*dynamodb.DescribeGlobalTableSettingsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeKinesisStreamingDestination(*dynamodb.DescribeKinesisStreamingDestinationInput) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeKinesisStreamingDestinationWithContext(context.Context, *dynamodb.DescribeKinesisStreamingDestinationInput, ...request.Option) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeLimits(*dynamodb.DescribeLimitsInput) (*dynamodb.DescribeLimitsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeLimitsWithContext(context.Context, *dynamodb.DescribeLimitsInput, ...request.Option) (*dynamodb.DescribeLimitsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeTableWithContext(context.Context, *dynamodb.DescribeTableInput, ...request.Option) (*dynamodb.DescribeTableOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeTableReplicaAutoScaling(*dynamodb.DescribeTableReplicaAutoScalingInput) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeTableReplicaAutoScalingWithContext(context.Context, *dynamodb.DescribeTableReplicaAutoScalingInput, ...request.Option) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DescribeTimeToLive(*dynamodb.DescribeTimeToLiveInput) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DescribeTimeToLiveWithContext(context.Context, *dynamodb.DescribeTimeToLiveInput, ...request.Option) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) DisableKinesisStreamingDestination(*dynamodb.DisableKinesisStreamingDestinationInput) (*dynamodb.DisableKinesisStreamingDestinationOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) DisableKinesisStreamingDestinationWithContext(context.Context, *dynamodb.DisableKinesisStreamingDestinationInput, ...request.Option) (*dynamodb.DisableKinesisStreamingDestinationOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) EnableKinesisStreamingDestination(*dynamodb.EnableKinesisStreamingDestinationInput) (*dynamodb.EnableKinesisStreamingDestinationOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) EnableKinesisStreamingDestinationWithContext(context.Context, *dynamodb.EnableKinesisStreamingDestinationInput, ...request.Option) (*dynamodb.EnableKinesisStreamingDestinationOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ExecuteStatement(*dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ExecuteStatementWithContext(context.Context, *dynamodb.ExecuteStatementInput, ...request.Option) (*dynamodb.ExecuteStatementOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ExecuteTransaction(*dynamodb.ExecuteTransactionInput) (*dynamodb.ExecuteTransactionOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ExecuteTransactionWithContext(context.Context, *dynamodb.ExecuteTransactionInput, ...request.Option) (*dynamodb.ExecuteTransactionOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ExportTableToPointInTime(*dynamodb.ExportTableToPointInTimeInput) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ExportTableToPointInTimeWithContext(context.Context, *dynamodb.ExportTableToPointInTimeInput, ...request.Option) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) GetItemWithContext(context.Context, *dynamodb.GetItemInput, ...request.Option) (*dynamodb.GetItemOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ListBackups(*dynamodb.ListBackupsInput) (*dynamodb.ListBackupsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ListBackupsWithContext(context.Context, *dynamodb.ListBackupsInput, ...request.Option) (*dynamodb.ListBackupsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ListContributorInsights(*dynamodb.ListContributorInsightsInput) (*dynamodb.ListContributorInsightsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ListContributorInsightsWithContext(context.Context, *dynamodb.ListContributorInsightsInput, ...request.Option) (*dynamodb.ListContributorInsightsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ListContributorInsightsPages(*dynamodb.ListContributorInsightsInput, func(*dynamodb.ListContributorInsightsOutput, bool) bool) error {
	return ToBeImplementedErr
}
func (db *DonutDB) ListContributorInsightsPagesWithContext(context.Context, *dynamodb.ListContributorInsightsInput, func(*dynamodb.ListContributorInsightsOutput, bool) bool, ...request.Option) error {
	return ToBeImplementedErr
}

func (db *DonutDB) ListExports(*dynamodb.ListExportsInput) (*dynamodb.ListExportsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ListExportsWithContext(context.Context, *dynamodb.ListExportsInput, ...request.Option) (*dynamodb.ListExportsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ListExportsPages(*dynamodb.ListExportsInput, func(*dynamodb.ListExportsOutput, bool) bool) error {
	return ToBeImplementedErr
}
func (db *DonutDB) ListExportsPagesWithContext(context.Context, *dynamodb.ListExportsInput, func(*dynamodb.ListExportsOutput, bool) bool, ...request.Option) error {
	return ToBeImplementedErr
}

func (db *DonutDB) ListGlobalTables(*dynamodb.ListGlobalTablesInput) (*dynamodb.ListGlobalTablesOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ListGlobalTablesWithContext(context.Context, *dynamodb.ListGlobalTablesInput, ...request.Option) (*dynamodb.ListGlobalTablesOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ListTagsOfResource(*dynamodb.ListTagsOfResourceInput) (*dynamodb.ListTagsOfResourceOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ListTagsOfResourceWithContext(context.Context, *dynamodb.ListTagsOfResourceInput, ...request.Option) (*dynamodb.ListTagsOfResourceOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) PutItemWithContext(context.Context, *dynamodb.PutItemInput, ...request.Option) (*dynamodb.PutItemOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) Query(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) QueryWithContext(context.Context, *dynamodb.QueryInput, ...request.Option) (*dynamodb.QueryOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) QueryPages(*dynamodb.QueryInput, func(*dynamodb.QueryOutput, bool) bool) error {
	return ToBeImplementedErr
}
func (db *DonutDB) QueryPagesWithContext(context.Context, *dynamodb.QueryInput, func(*dynamodb.QueryOutput, bool) bool, ...request.Option) error {
	return ToBeImplementedErr
}

func (db *DonutDB) RestoreTableFromBackup(*dynamodb.RestoreTableFromBackupInput) (*dynamodb.RestoreTableFromBackupOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) RestoreTableFromBackupWithContext(context.Context, *dynamodb.RestoreTableFromBackupInput, ...request.Option) (*dynamodb.RestoreTableFromBackupOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) RestoreTableToPointInTime(*dynamodb.RestoreTableToPointInTimeInput) (*dynamodb.RestoreTableToPointInTimeOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) RestoreTableToPointInTimeWithContext(context.Context, *dynamodb.RestoreTableToPointInTimeInput, ...request.Option) (*dynamodb.RestoreTableToPointInTimeOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) ScanWithContext(context.Context, *dynamodb.ScanInput, ...request.Option) (*dynamodb.ScanOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) ScanPages(*dynamodb.ScanInput, func(*dynamodb.ScanOutput, bool) bool) error {
	return ToBeImplementedErr
}
func (db *DonutDB) ScanPagesWithContext(context.Context, *dynamodb.ScanInput, func(*dynamodb.ScanOutput, bool) bool, ...request.Option) error {
	return ToBeImplementedErr
}

func (db *DonutDB) TagResource(*dynamodb.TagResourceInput) (*dynamodb.TagResourceOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) TagResourceWithContext(context.Context, *dynamodb.TagResourceInput, ...request.Option) (*dynamodb.TagResourceOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) TransactGetItems(*dynamodb.TransactGetItemsInput) (*dynamodb.TransactGetItemsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) TransactGetItemsWithContext(context.Context, *dynamodb.TransactGetItemsInput, ...request.Option) (*dynamodb.TransactGetItemsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) TransactWriteItems(*dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) TransactWriteItemsWithContext(context.Context, *dynamodb.TransactWriteItemsInput, ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UntagResource(*dynamodb.UntagResourceInput) (*dynamodb.UntagResourceOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UntagResourceWithContext(context.Context, *dynamodb.UntagResourceInput, ...request.Option) (*dynamodb.UntagResourceOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateContinuousBackups(*dynamodb.UpdateContinuousBackupsInput) (*dynamodb.UpdateContinuousBackupsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateContinuousBackupsWithContext(context.Context, *dynamodb.UpdateContinuousBackupsInput, ...request.Option) (*dynamodb.UpdateContinuousBackupsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateContributorInsights(*dynamodb.UpdateContributorInsightsInput) (*dynamodb.UpdateContributorInsightsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateContributorInsightsWithContext(context.Context, *dynamodb.UpdateContributorInsightsInput, ...request.Option) (*dynamodb.UpdateContributorInsightsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateGlobalTable(*dynamodb.UpdateGlobalTableInput) (*dynamodb.UpdateGlobalTableOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateGlobalTableWithContext(context.Context, *dynamodb.UpdateGlobalTableInput, ...request.Option) (*dynamodb.UpdateGlobalTableOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateGlobalTableSettings(*dynamodb.UpdateGlobalTableSettingsInput) (*dynamodb.UpdateGlobalTableSettingsOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateGlobalTableSettingsWithContext(context.Context, *dynamodb.UpdateGlobalTableSettingsInput, ...request.Option) (*dynamodb.UpdateGlobalTableSettingsOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateItemWithContext(context.Context, *dynamodb.UpdateItemInput, ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateTable(*dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateTableWithContext(context.Context, *dynamodb.UpdateTableInput, ...request.Option) (*dynamodb.UpdateTableOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateTableReplicaAutoScaling(*dynamodb.UpdateTableReplicaAutoScalingInput) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateTableReplicaAutoScalingWithContext(context.Context, *dynamodb.UpdateTableReplicaAutoScalingInput, ...request.Option) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) UpdateTimeToLive(*dynamodb.UpdateTimeToLiveInput) (*dynamodb.UpdateTimeToLiveOutput, error) {
	return nil, ToBeImplementedErr
}
func (db *DonutDB) UpdateTimeToLiveWithContext(context.Context, *dynamodb.UpdateTimeToLiveInput, ...request.Option) (*dynamodb.UpdateTimeToLiveOutput, error) {
	return nil, ToBeImplementedErr
}

func (db *DonutDB) WaitUntilTableExists(*dynamodb.DescribeTableInput) error {
	return ToBeImplementedErr
}
func (db *DonutDB) WaitUntilTableExistsWithContext(context.Context, *dynamodb.DescribeTableInput, ...request.WaiterOption) error {
	return ToBeImplementedErr
}

func (db *DonutDB) WaitUntilTableNotExists(*dynamodb.DescribeTableInput) error {
	return ToBeImplementedErr
}
func (db *DonutDB) WaitUntilTableNotExistsWithContext(context.Context, *dynamodb.DescribeTableInput, ...request.WaiterOption) error {
	return ToBeImplementedErr
}

// *Request() methods don't make sense for us to implement, but we have stub methods so we can be used as a dynamodbiface.DynamoDBAPI

func (db *DonutDB) BatchExecuteStatementRequest(*dynamodb.BatchExecuteStatementInput) (*request.Request, *dynamodb.BatchExecuteStatementOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) BatchGetItemRequest(*dynamodb.BatchGetItemInput) (*request.Request, *dynamodb.BatchGetItemOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) BatchWriteItemRequest(*dynamodb.BatchWriteItemInput) (*request.Request, *dynamodb.BatchWriteItemOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) CreateBackupRequest(*dynamodb.CreateBackupInput) (*request.Request, *dynamodb.CreateBackupOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) CreateGlobalTableRequest(*dynamodb.CreateGlobalTableInput) (*request.Request, *dynamodb.CreateGlobalTableOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) CreateTableRequest(*dynamodb.CreateTableInput) (*request.Request, *dynamodb.CreateTableOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DeleteBackupRequest(*dynamodb.DeleteBackupInput) (*request.Request, *dynamodb.DeleteBackupOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DeleteItemRequest(*dynamodb.DeleteItemInput) (*request.Request, *dynamodb.DeleteItemOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DeleteTableRequest(*dynamodb.DeleteTableInput) (*request.Request, *dynamodb.DeleteTableOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeBackupRequest(*dynamodb.DescribeBackupInput) (*request.Request, *dynamodb.DescribeBackupOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeContinuousBackupsRequest(*dynamodb.DescribeContinuousBackupsInput) (*request.Request, *dynamodb.DescribeContinuousBackupsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeContributorInsightsRequest(*dynamodb.DescribeContributorInsightsInput) (*request.Request, *dynamodb.DescribeContributorInsightsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeEndpointsRequest(*dynamodb.DescribeEndpointsInput) (*request.Request, *dynamodb.DescribeEndpointsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeExportRequest(*dynamodb.DescribeExportInput) (*request.Request, *dynamodb.DescribeExportOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeGlobalTableRequest(*dynamodb.DescribeGlobalTableInput) (*request.Request, *dynamodb.DescribeGlobalTableOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeGlobalTableSettingsRequest(*dynamodb.DescribeGlobalTableSettingsInput) (*request.Request, *dynamodb.DescribeGlobalTableSettingsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeKinesisStreamingDestinationRequest(*dynamodb.DescribeKinesisStreamingDestinationInput) (*request.Request, *dynamodb.DescribeKinesisStreamingDestinationOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeLimitsRequest(*dynamodb.DescribeLimitsInput) (*request.Request, *dynamodb.DescribeLimitsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeTableRequest(*dynamodb.DescribeTableInput) (*request.Request, *dynamodb.DescribeTableOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeTableReplicaAutoScalingRequest(*dynamodb.DescribeTableReplicaAutoScalingInput) (*request.Request, *dynamodb.DescribeTableReplicaAutoScalingOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DescribeTimeToLiveRequest(*dynamodb.DescribeTimeToLiveInput) (*request.Request, *dynamodb.DescribeTimeToLiveOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) DisableKinesisStreamingDestinationRequest(*dynamodb.DisableKinesisStreamingDestinationInput) (*request.Request, *dynamodb.DisableKinesisStreamingDestinationOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) EnableKinesisStreamingDestinationRequest(*dynamodb.EnableKinesisStreamingDestinationInput) (*request.Request, *dynamodb.EnableKinesisStreamingDestinationOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ExecuteStatementRequest(*dynamodb.ExecuteStatementInput) (*request.Request, *dynamodb.ExecuteStatementOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ExecuteTransactionRequest(*dynamodb.ExecuteTransactionInput) (*request.Request, *dynamodb.ExecuteTransactionOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ExportTableToPointInTimeRequest(*dynamodb.ExportTableToPointInTimeInput) (*request.Request, *dynamodb.ExportTableToPointInTimeOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) GetItemRequest(*dynamodb.GetItemInput) (*request.Request, *dynamodb.GetItemOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ListBackupsRequest(*dynamodb.ListBackupsInput) (*request.Request, *dynamodb.ListBackupsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ListContributorInsightsRequest(*dynamodb.ListContributorInsightsInput) (*request.Request, *dynamodb.ListContributorInsightsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ListExportsRequest(*dynamodb.ListExportsInput) (*request.Request, *dynamodb.ListExportsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ListGlobalTablesRequest(*dynamodb.ListGlobalTablesInput) (*request.Request, *dynamodb.ListGlobalTablesOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ListTablesRequest(*dynamodb.ListTablesInput) (*request.Request, *dynamodb.ListTablesOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ListTagsOfResourceRequest(*dynamodb.ListTagsOfResourceInput) (*request.Request, *dynamodb.ListTagsOfResourceOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) PutItemRequest(*dynamodb.PutItemInput) (*request.Request, *dynamodb.PutItemOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) QueryRequest(*dynamodb.QueryInput) (*request.Request, *dynamodb.QueryOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) RestoreTableFromBackupRequest(*dynamodb.RestoreTableFromBackupInput) (*request.Request, *dynamodb.RestoreTableFromBackupOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) RestoreTableToPointInTimeRequest(*dynamodb.RestoreTableToPointInTimeInput) (*request.Request, *dynamodb.RestoreTableToPointInTimeOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) ScanRequest(*dynamodb.ScanInput) (*request.Request, *dynamodb.ScanOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) TagResourceRequest(*dynamodb.TagResourceInput) (*request.Request, *dynamodb.TagResourceOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) TransactGetItemsRequest(*dynamodb.TransactGetItemsInput) (*request.Request, *dynamodb.TransactGetItemsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) TransactWriteItemsRequest(*dynamodb.TransactWriteItemsInput) (*request.Request, *dynamodb.TransactWriteItemsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UntagResourceRequest(*dynamodb.UntagResourceInput) (*request.Request, *dynamodb.UntagResourceOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateContinuousBackupsRequest(*dynamodb.UpdateContinuousBackupsInput) (*request.Request, *dynamodb.UpdateContinuousBackupsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateContributorInsightsRequest(*dynamodb.UpdateContributorInsightsInput) (*request.Request, *dynamodb.UpdateContributorInsightsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateGlobalTableRequest(*dynamodb.UpdateGlobalTableInput) (*request.Request, *dynamodb.UpdateGlobalTableOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateGlobalTableSettingsRequest(*dynamodb.UpdateGlobalTableSettingsInput) (*request.Request, *dynamodb.UpdateGlobalTableSettingsOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateItemRequest(*dynamodb.UpdateItemInput) (*request.Request, *dynamodb.UpdateItemOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateTableRequest(*dynamodb.UpdateTableInput) (*request.Request, *dynamodb.UpdateTableOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateTableReplicaAutoScalingRequest(*dynamodb.UpdateTableReplicaAutoScalingInput) (*request.Request, *dynamodb.UpdateTableReplicaAutoScalingOutput) {
	panic(UnimplementedErr)
	return nil, nil
}

func (db *DonutDB) UpdateTimeToLiveRequest(*dynamodb.UpdateTimeToLiveInput) (*request.Request, *dynamodb.UpdateTimeToLiveOutput) {
	panic(UnimplementedErr)
	return nil, nil
}
