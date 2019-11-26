using CommandLine;

namespace ApplyPosixPermissions.Models
{
    internal class CommandLineOptions
    {
        [Option("StorageAccountName", Required = true, HelpText = "Azure Data Lake Gen 2 Storage Account Name.")]
        public string StorageAccountName { get; set; }

        [Option("SharedAccessKey", Required = true, HelpText = "Azure Data Lake Gen 2 Storage Account Shared Access Key.")]
        public string SharedAccessKey { get; set; }

        [Option("ConfigurationPath", Required = true, HelpText = "Path of POSIX JSON configuration file.")]
        public string ConfigurationPath { get; set; }

        [Option("DefaultConnectionLimit", Required = false, HelpText = "Service Point Manager default connection limit.")]
        public int? DefaultConnectionLimit { get; set; }

        [Option("Expect100Continue", Required = false, HelpText = "Service Point Manager expect 100 continue.")]
        public bool? Expect100Continue { get; set; }
    }
}
