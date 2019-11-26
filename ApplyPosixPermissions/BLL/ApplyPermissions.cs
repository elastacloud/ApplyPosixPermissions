using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ApplyPosixPermissions.Interfaces;
using ApplyPosixPermissions.Models;
using ApplyPosixPermissions.Wrappers;
using NLog;
using Storage.Net;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2;

namespace ApplyPosixPermissions.BLL
{
    public class ApplyPermissions
    {
        private readonly ILogger _logger;
        private readonly IAzureDataLakeGen2BlobStorageWrapper _storage;

        public ApplyPermissions(string accountName, string accessKey)
        {
            var client = (IAzureDataLakeGen2BlobStorage)StorageFactory.Blobs.AzureDataLakeGen2StoreBySharedAccessKey(accountName, accessKey);
            var wrapper = new AzureDataLakeGen2BlobStorageWrapper(client);
            _storage = new RetryableAzureDataLakeGen2BlobStorageWrapper(wrapper);
            _logger = LogManager.GetCurrentClassLogger();
        }

        public ApplyPermissions(IAzureDataLakeGen2BlobStorageWrapper storage, ILogger logger)
        {
            _storage = storage;
            _logger = logger;
        }

        public async Task ProcessAsync(List<DataLakeDirectory> directories,
            CancellationToken cancellationToken = default)
        {
            try
            {
                foreach (var directory in directories)
                {
                    await new ApplyDirectory(_storage, _logger, directory, cancellationToken).ProcessAsync();
                }
            }
            catch (Exception e)
            {
                _logger.Error(e);
                throw;
            }
        }
    }
}