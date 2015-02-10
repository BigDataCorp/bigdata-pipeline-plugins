using System;
using System.Collections.Generic;
using System.IO;

namespace FileListenerPlugin
{
    public class StreamTransfer
    {
        public string FileName { get; set; }
        public Stream FileStream { get; set; }
    }

    public class FileTransferDetails : BigDataPipeline.FlexibleObject
    {
        public const int DefaultReadBufferSize = 2 * 1024 * 1024;
        public const int DefaultWriteBufferSize = 512 * 1024;

        public string ConnectionString { get; set; }

        /// <summary>
        /// Where the file is located.
        /// </summary>
        public FileLocation Location { get; set; }

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

        public string Login { get; set; }

        public string Password { get; set; }

        public string Server { get; set; }

        public int Port { get; set; }

        public int RetryCount { get; set; }

        public int RetryWaitMs { get; set; }

        public bool SearchTopDirectoryOnly { get; set; }

        public FileTransferDetails ()
        {
            RetryCount = 3;
            RetryWaitMs = 500;
            SearchTopDirectoryOnly = true;
        }

        /// <summary>
        /// 
        /// ftp://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// ftps://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// ftpes://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// sftp://[login:password@][server][:port]/[path]/[file name or wildcard expression]
        /// s3://[login:password@][region endpoint]/[bucketname]/[path]/[file name or wildcard expression]
        /// s3://[login:password@][s3-us-west-1.amazonaws.com]/[bucketname]/[path]/[file name or wildcard expression]
        /// http://[server][:port]/[path]/[file name or wildcard expression]
        /// https://[server][:port]/[path]/[file name or wildcard expression]
        /// c:/[path]/[file name or wildcard expression]
        /// 
        /// </summary>
        public static FileTransferDetails ParseSearchPath (string inputSearchPath)
        {
            FileTransferDetails obj = new FileTransferDetails ();
            // sanity check
            if (string.IsNullOrWhiteSpace (inputSearchPath))
                return obj;

            inputSearchPath = inputSearchPath.Trim ().Replace ("\\", "/");
            obj.ConnectionString = inputSearchPath;

            // s3://BucketName/directory_like_path/*.txt
            var path = SplitPrefix (inputSearchPath);
            
            switch (path[0])
            {
                case "s3":
                    obj.Location = FileLocation.S3;                    
                    break;
                case "http":
                case "https":
                    obj.Location = FileLocation.HTTP;
                    obj.FilePath = inputSearchPath;
                    break;
                case "ftp":
                    obj.Location = FileLocation.FTP;
                    break;
                case "ftps":
                    obj.Location = FileLocation.FTPS;                    
                    break;
                case "ftpes":
                    obj.Location = FileLocation.FTPES;
                    break;
                case "sftp":
                    obj.Location = FileLocation.SFTP;
                    break;
                default:
                    obj.Location = FileLocation.FileSystem;
                    break;
            }

            ParsePath (obj, inputSearchPath);

            return obj;
        }

        private static string[] SplitPrefix (string input)
        {
            string[] path = new string[2];
            var ix = input.IndexOf ("://", StringComparison.Ordinal);
            var len = "://".Length;
            if (ix < 0)
            {
                ix = input.IndexOf (":/", StringComparison.Ordinal);
                len = ":/".Length;
            }

            if (ix > 0)
            {
                path[0] = input.Substring (0, ix).ToLowerInvariant ();
                path[1] = input.Substring (path[0].Length + len);
            }
            else
            {
                path[0] = "";
                path[1] = input;
            }
            return path;
        }

        private static void ParsePath (FileTransferDetails obj, string inputSearchPath)
        {
            try
            {
                // FileSystem and HTTP uses full path to access the file
                if (obj.Location == FileLocation.HTTP)
                {
                    obj.UseWildCardSearch = false;
                    obj.FilePath = inputSearchPath;
                    return;
                }
                else if (obj.Location == FileLocation.FileSystem)
                {
                    obj.UseWildCardSearch = false;
                    obj.FilePath = System.IO.Path.GetDirectoryName (inputSearchPath).Replace ('\\', '/');
                    obj.SearchPattern = System.IO.Path.GetFileName (inputSearchPath);
                    return;
                }

                var splitedPath = SplitPrefix (inputSearchPath);
                var path = splitedPath[1];
            
                // extract login and password
                var i = path.IndexOf ('/');
                var j = path.IndexOf ('@');
                if (j >= 0 && j < i)
                {
                    var credentials = path.Substring (0, j).Split (':');
                    obj.Login = credentials[0];
                    obj.Password = credentials[1];

                    path = path.Substring (j + 1);
                }
            
                // extract s3 bucket name and region
                if (obj.Location == FileLocation.S3)
                {
                    i = path.IndexOf ('/');
                    if (i < 0)
                        throw new Exception ("Invalid S3 file search path");
                    obj.Set("bucketName", path.Substring (0, i));

                    // aws regions: http://docs.aws.amazon.com/general/latest/gr/rande.html
                    j = path.IndexOf (".amazonaws.com", StringComparison.OrdinalIgnoreCase);
                    if (j >= 0)
                    {
                        obj.Set ("awsRegion", path.Substring (0, j).Replace ("s3.", "").Replace ("s3-", "").Trim ('-').Trim ());
                    }
                
                    path = path.Substring (i + 1);
                }
                else // FTP & SFTP
                {
                    // extract server and port
                    i = path.IndexOf ('/');
                    var split = path.Substring (0, i).Split (':');
                    obj.Server = split[0];
                    if (split.Length > 1)
                    {
                        int p = 0;
                        if (Int32.TryParse (split[1], out p))
                            obj.Port = p;                    
                    }

                    if (obj.Port <= 0)
                    {
                        if (obj.Location == FileLocation.FTP)
                            obj.Port = 21;
                        else if (obj.Location == FileLocation.FTPS)
                            obj.Port = 990;
                        else if (obj.Location == FileLocation.FTPES)
                            obj.Port = 21;
                        else if (obj.Location == FileLocation.SFTP)
                            obj.Port = 22;
                    }
                
                    path = path.Substring (i + 1);
                }

                if (!String.IsNullOrEmpty (path))
                {
                    // find file wildcard:
                    var wildCard1 = path.IndexOf ('*');
                    var wildCard2 = path.IndexOf ('?');
                    if (wildCard1 > 0 || wildCard2 > 0)
                    {
                        obj.UseWildCardSearch = true;
                        if (obj.Location == FileLocation.S3)
                        {
                            int endPos = (wildCard1 > 0 && wildCard2 > 0) ? Math.Min (wildCard1, wildCard2) : Math.Max (wildCard1, wildCard2);
                            obj.FilePath = path.Substring (0, endPos);
                            obj.SearchPattern = WildcardToRegex (path);                    
                        }
                        else
                        {
                            obj.FilePath = System.IO.Path.GetDirectoryName (path).Replace ('\\', '/');
                            obj.SearchPattern = WildcardToRegex (path);
                        }
                    }
                    else if (obj.Location == FileLocation.S3)
                    {
                        obj.UseWildCardSearch = false;
                        obj.FilePath = path;
                    }
                    else
                    {
                        obj.UseWildCardSearch = false;                    
                        obj.FilePath = System.IO.Path.GetDirectoryName (path).Replace ('\\', '/');
                        obj.SearchPattern = System.IO.Path.GetFileName (path);
                    }
                }
                else
                {
                    obj.UseWildCardSearch = false;
                    obj.FilePath = "";
                    obj.SearchPattern = "";
                }
            }
            finally
            {
               if (obj.Location != FileLocation.HTTP && obj.FilePath != null && !obj.FilePath.EndsWith ("/"))
                    obj.FilePath = obj.FilePath + '/';
            }
        }

        public static string WildcardToRegex (string pattern)
        {
            // (new System.Text.RegularExpressions.Regex (pattern,RegexOptions.IgnoreCase)).IsMatch (txt)

            return "^" + System.Text.RegularExpressions.Regex.Escape (pattern).
                               Replace (@"\*", ".*").
                               Replace (@"\?", ".") + "$";
        }

        public static void DeleteFile(string fileName)
        {
            if (fileName == null)
                return;
            try { System.IO.File.Delete (fileName); }
            catch { }
        }

        public static void CreateDirectory (string path)
        {
            if (path == null)
                return;
            try { System.IO.Directory.CreateDirectory (System.IO.Path.GetDirectoryName (path)); }
            catch { }
        }

    }
}