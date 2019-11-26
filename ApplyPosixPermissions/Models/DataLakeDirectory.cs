using System.Collections.Generic;

namespace ApplyPosixPermissions.Models
{
    public class DataLakeDirectory
    {
        public string Path { get; set; }
        public bool Upn { get; set; }
        public bool Recurse { get; set; }
        public bool Force { get; set; }
        public List<Acl> Acls { get; set; }
    }
}