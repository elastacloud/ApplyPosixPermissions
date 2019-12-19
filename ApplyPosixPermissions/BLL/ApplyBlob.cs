using ApplyPosixPermissions.Interfaces;
using Storage.Net.Blobs;
using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ApplyPosixPermissions.BLL
{
    internal class ApplyBlob
    {
        private readonly IEnumerable<AclEntry> _acls;
        private readonly IEnumerable<AclEntry> _defaultAcls;
        private readonly Blob _blob;
        private readonly IAzureDataLakeGen2BlobStorageWrapper _storage;
        private readonly bool _upn;

        public ApplyBlob(IAzureDataLakeGen2BlobStorageWrapper storage, Blob blob, IEnumerable<AclEntry> acls, IEnumerable<AclEntry> defaultAcls, bool upn)
        {
            _storage = storage;
            _blob = blob;
            _acls = acls;
            _defaultAcls = defaultAcls;
            _upn = upn;
        }

        public async Task ProcessAsync()
        {
            var accessControl = await _storage.GetAccessControlAsync(_blob.FullPath, _upn);
            accessControl.Acl.Clear();

            foreach (var acl in _acls)
            {
                accessControl.Acl.Add(acl);
            }

            if (_blob.IsFolder)
            {
                accessControl.DefaultAcl.Clear();

                foreach (var acl in _defaultAcls)
                {
                    accessControl.DefaultAcl.Add(acl);
                }
            }

            await _storage.SetAccessControlAsync(_blob.FullPath, accessControl);
        }
    }
}