using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWSRedshiftPlugin
{
    public class FileTransferDetails
    {
        public enum FileLocation { FileSystem, S3, FTP, HTTP }

        /// <summary>
        /// Where the file is located.
        /// </summary>
        public FileLocation Location { get; set; }

        /// <summary>
        /// Name of the AWS S3 bucket (if file is in S3).
        /// </summary>
        public string BucketName { get; set; }

        /// <summary>
        /// If the path has a Wildcard search pattern.
        /// </summary>
        public bool UseWildCardSearch { get; set; }

        /// <summary>
        /// Regex search pattern generated from a wildcard like pattern.
        /// </summary>
        public string SearchPattern { get; set; }

        /// <summary>
        /// The file path without the SearchPattern.
        /// </summary>
        public string FilePath { get; set; }

        public static FileTransferDetails ParseSearchPath (string inputSearchPath)
        {
            FileTransferDetails obj = new FileTransferDetails ();
            // sanity check
            if (string.IsNullOrWhiteSpace (inputSearchPath))
                return obj;

            inputSearchPath = inputSearchPath.Trim ().Replace ("\\", "/");

            // s3://BucketName/directory_like_path/*.sql
            var prefix = GetPrefix (inputSearchPath);

            if (String.IsNullOrEmpty (prefix))
                throw new ArgumentException ("Invalid path", "inputSearchPath");

            switch (prefix)
            {
                case "s3":
                    inputSearchPath = ParseAwsS3Path (obj, inputSearchPath);
                    break;
                case "http":
                case "https":
                case "ftp":
                case "ftps":
                case "sftp":
                default:
                    throw new NotImplementedException ();
            }            

            return obj;
        }

        private static string GetPrefix (string input)
        {
            var ix = input.IndexOf ("://", StringComparison.Ordinal);
            if (ix > 0)
            {
                return input.Substring (0, ix).ToLowerInvariant ();
            }
            ix = input.IndexOf (":/", StringComparison.Ordinal);
            if (ix > 0)
            {
                return input.Substring (0, ix).ToLowerInvariant ();
            }
            return null;
        }
  
        private static string ParseAwsS3Path (FileTransferDetails obj, string inputSearchPath)
        {
            inputSearchPath = inputSearchPath.Substring ("s3://".Length);
            // extract bucketname
            var idx = inputSearchPath.IndexOf ('/');
            if (idx < 0)
                throw new Exception ("Invalid S3 file search path");

            obj.Location = FileTransferDetails.FileLocation.S3;
            obj.BucketName = inputSearchPath.Substring (0, idx);

            // find file wildcard:
            var wildCard1 = inputSearchPath.IndexOf ('*');
            var wildCard2 = inputSearchPath.IndexOf ('?');
            if (wildCard1 > 0 || wildCard2 > 0)
            {
                int endPos;
                if (wildCard1 > 0 && wildCard2 > 0)
                    endPos = Math.Min (wildCard1, wildCard2);
                else if (wildCard1 > 0)
                    endPos = wildCard1;
                else
                    endPos = wildCard2;

                obj.UseWildCardSearch = true;
                obj.SearchPattern = WildcardToRegex (inputSearchPath.Substring (idx + 1));
                obj.FilePath = inputSearchPath.Substring (idx + 1, endPos - (idx + 1));
            }
            else
            {
                obj.UseWildCardSearch = false;
                obj.FilePath = inputSearchPath.Substring (idx + 1);
            }
            return inputSearchPath;
        }

        public static string WildcardToRegex (string pattern)
        {
            // (new System.Text.RegularExpressions.Regex (pattern,RegexOptions.IgnoreCase)).IsMatch (txt)

            return "^" + System.Text.RegularExpressions.Regex.Escape (pattern).
                               Replace (@"\*", ".*").
                               Replace (@"\?", ".") + "$";
        }

    }

}
