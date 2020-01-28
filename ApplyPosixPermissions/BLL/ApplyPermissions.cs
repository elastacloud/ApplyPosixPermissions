using ApplyPosixPermissions.Interfaces;
using ApplyPosixPermissions.Models;
using ApplyPosixPermissions.Wrappers;
using NLog;
using Storage.Net;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.BLL
{
    public class ApplyPermissions
    {
        private readonly ILogger _logger;
        private readonly IAzureDataLakeStorageWrapper _storage;

        public ApplyPermissions(string accountName, string accessKey)
        {
            var client = StorageFactory.Blobs.AzureDataLakeStorageWithSharedKey(accountName, accessKey);
            _storage = new RetryableAzureDataLakeStorageWrapper(client);
            _logger = LogManager.GetCurrentClassLogger();
        }

        public ApplyPermissions(IAzureDataLakeStorageWrapper storage, ILogger logger)
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