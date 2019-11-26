using Storage.Net.Microsoft.Azure.DataLake.Store.Gen2.Model;

namespace ApplyPosixPermissions.Models
{
    public class Acl
    {
        public ObjectType ObjectType { get; set; }
        public string Identity { get; set; }
        public bool Read { get; set; }
        public bool Write { get; set; }
        public bool Execute { get; set; }
        public bool DefaultRead { get; set; }
        public bool DefaultWrite { get; set; }
        public bool DefaultExecute { get; set; }
    }
}