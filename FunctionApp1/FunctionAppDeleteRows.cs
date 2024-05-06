using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace FunctionApp1
{
    public class FunctionAppDeleteRows
    {
        [FunctionName("FunctionAppDeleteRows")]
        public async Task Run([TimerTrigger("0 */1 * * * *")]TimerInfo myTimer, ILogger log)
        {
            //0 0 0 * * Mon
            log.LogTrace("Empieza proceso de borrado");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
            
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            
            await DeleteAllRows<TableEntity>(Environment.GetEnvironmentVariable("TableName"), tableClient);
            log.LogTrace("Finaliza proceso de borrado");
            
        }

        private async Task DeleteAllRows<T>(string tableName, CloudTableClient client) where T : ITableEntity, new()
        {
            CloudTable tableRef = client.GetTableReference(tableName);
            var query = new TableQuery<T>();
            TableContinuationToken token = null;
            do
            {
                var result = await tableRef.ExecuteQuerySegmentedAsync(query, token);
                foreach (var row in result)
                {
                    var op = TableOperation.Delete(row);
                    await tableRef.ExecuteAsync(op);
                }
                token = result.ContinuationToken;
            } while (token != null);
        }
    }
}
