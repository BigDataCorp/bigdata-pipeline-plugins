using System;
using System.Linq;
using System.IO;
using System.Net;
using System.Net.FtpClient;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using BigDataPipeline.Interfaces;

namespace FileListenerPlugin
{
    public class FTPTransfer : IFileTransfer 
    {
        FtpClient client;

        static string[] serviceSchemes = new[] { "ftp", "ftps", "fptes" };

        /// <summary>
        /// Gets the URI scheme names that this intance can handle.
        /// </summary>
        public IEnumerable<string> GetSchemeNames ()
        {
            return serviceSchemes;
        }

        public FileServiceConnectionInfo ParseConnectionUri (string connectionUri, IEnumerable<KeyValuePair<string, string>> extraOptions)
        {
            var info = new FileServiceConnectionInfo (connectionUri, extraOptions);

            // parse host
            if (info.Port <= 0)
            {
                if (info.Location == "ftp")
                    info.Port = 21;
                else if (info.Location == "ftps")
                    info.Port = 990;
                else if (info.Location == "ftpes")
                    info.Port = 21;
            }

            return info;
        }

        public FileServiceConnectionInfo Details { get; private set; }

        public bool Status { get; private set; }

        public string LastError { get; private set; }

        private void _setStatus (Exception message)
        {
            string msg = null;
            if (message != null && message.Message != null)
            {
                msg = message.Message;
                if (message.InnerException != null && message.InnerException.Message != null)
                    msg += "; " + message.InnerException.Message;
            }
            
            _setStatus (msg != null, msg);
        }

        private void _setStatus (bool status, string message = null)
        {
            Status = status;
            LastError = message;
        }
                
        public bool Open (FileServiceConnectionInfo details)
        { 
            Details = details;
            if (Details.RetryCount <= 0)
                Details.RetryCount = 1;
            return Open ();
        }
        
        private bool Open ()
        {
            if (IsOpened ())
                return true;

            var lastMode = Details.Location;
            // try to apply some fixes
            if (Details.Port <= 0)
                Details.Port = Details.Location == "ftps" ? 990 : 21;

            // check for empty login/password
            if (String.IsNullOrEmpty (Details.Login) && String.IsNullOrEmpty (Details.Password))
            {
                Details.Login = Details.Get ("ftpLogin", Details.Login);
                Details.Password = Details.Get ("ftpPassword", Details.Password);
            }

            // Default, use the EPSV command for data channels. Other
            // options are Passive (PASV), ExtenedActive (EPRT) and
            // Active (PORT). EPSV should work for most people against
            // a properly configured server and firewall.
            var list = new List<Tuple<string, FtpDataConnectionType>> ()
                {
                    Tuple.Create ("ftp", FtpDataConnectionType.AutoPassive),
                    Tuple.Create ("ftp", FtpDataConnectionType.AutoActive),
                    Tuple.Create ("ftps", FtpDataConnectionType.AutoPassive),
                    Tuple.Create ("ftps", FtpDataConnectionType.AutoActive),
                    Tuple.Create ("ftp", FtpDataConnectionType.AutoPassive),
                    Tuple.Create ("ftpes", FtpDataConnectionType.AutoActive)
                };
            
            list = list.OrderBy (i => i.Item1 == lastMode ? 1 : 2).ToList ();
            int retries = Details.RetryCount < 2 ? 2 : Details.RetryCount;

            for (var i = 0; i < retries; i++)
            { 
                var mode = list[i];
                try
                {
                    client = new FtpClient ();
                    
                    client.DataConnectionType = mode.Item2;

                    client.Host = Details.Host;
                    client.Port = Details.Port;
                    client.Credentials = new NetworkCredential (Details.Login, Details.Password);
                    client.ValidateCertificate += new FtpSslValidation (OnValidateCertificate);
                    client.SocketKeepAlive = true;
                    client.ReadTimeout = 40000;

                    client.EncryptionMode = mode.Item1 == "ftp" ? FtpEncryptionMode.None : mode.Item1 == "ftps" ? FtpEncryptionMode.Implicit : FtpEncryptionMode.Explicit;
                    
                    client.Connect ();

                    // go to dir
                    if (!String.IsNullOrEmpty (Details.BasePath))
                    {
                        //CreateDirectory (Details.BasePath);
                        client.SetWorkingDirectory (Details.BasePath);
                    }

                    _setStatus (true);
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    Close ();
                }
                if (client != null)
                    break;
                System.Threading.Thread.Sleep (Details.RetryWaitMs);
            }
            return IsOpened ();
        }

        static void OnValidateCertificate (FtpClient control, FtpSslValidationEventArgs e)
        {
            if (e.PolicyErrors != System.Net.Security.SslPolicyErrors.None)
            {
                // invalid cert, do you want to accept it?
                e.Accept = true;
            }
        }

        private void CreateDirectory (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                return;
            folder = PreparePath (folder);
            if (!client.DirectoryExists (folder.TrimEnd ('/') + '/'))
            {
                var folders = folder.Trim ('/').Split ('/');
                var current = "/";
                foreach (var f in folders)
                {
                    current += f.TrimEnd ('/') + '/';
                    if (!client.DirectoryExists (current))
                    {
                        client.CreateDirectory (current);
                    }
                }
            }
        }

        private string PreparePath (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                folder = "/";
            folder = folder.Replace ('\\', '/');
            if (!folder.StartsWith ("/"))
                folder = "/" + folder;
            return folder;
        }

        public bool IsOpened ()
        {
            return client != null;
        }

        public void Dispose ()
        {
            Close ();
        }

        private void Close ()
        {
            if (client != null)
            {
                try
                {
                    using (var ptr = client)
                    {
                        client = null;
                        ptr.ValidateCertificate -= new FtpSslValidation (OnValidateCertificate);
                        if (ptr.IsConnected)
                            ptr.Disconnect ();
                    }
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                }
            }
            client = null;
        }

        private IEnumerable<FtpListItem> _listFiles (string folder, System.Text.RegularExpressions.Regex pattern, bool recursive)
        {
            _setStatus (true);

            if (!Open ())
                yield break;

            folder = PreparePath (folder);

            foreach (var f in client.GetListing (folder))
                if (f.Type == FtpFileSystemObjectType.Directory && recursive && f.FullName.StartsWith (folder) && !f.FullName.EndsWith ("."))
                    foreach (var n in _listFiles (f.FullName, pattern, recursive))
                        yield return n;
                else if (f.Type == FtpFileSystemObjectType.File)
                    if (pattern == null || pattern.IsMatch (f.FullName))
                        yield return f;
        }

        public IEnumerable<FileTransferInfo> ListFiles ()
        {
            return ListFiles (Details.BasePath, Details.FullPath.TrimStart ('/'), !Details.SearchTopDirectoryOnly);
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, bool recursive)
        {
            return ListFiles (folder, null, recursive);
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, string fileMask, bool recursive)
        {
            var pattern = String.IsNullOrEmpty (fileMask) ? null : new System.Text.RegularExpressions.Regex (FileTransferHelpers.WildcardToRegex (fileMask), System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
            return _listFiles (folder, pattern, recursive).Select (i => new FileTransferInfo (i.FullName, i.Size, i.Created, i.Modified));
        }

        public StreamTransferInfo GetFileStream (string file)
        {
            _setStatus (true);
            // download files
            return new StreamTransferInfo
            {
                FileName = file,
                FileStream = client.OpenRead (file)
            };
        }

        public IEnumerable<StreamTransferInfo> GetFileStreams (string folder, string fileMask, bool recursive)
        {
            var pattern = String.IsNullOrEmpty (fileMask) ? null : new System.Text.RegularExpressions.Regex (FileTransferHelpers.WildcardToRegex (fileMask), System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

            // get files
            List<FtpListItem> files = null;
            for (int i = 0; i < Details.RetryCount; i++)
            {
                // try to open
                if (!Open ())
                    continue;
                try
                {
                    // get all files to download, but limit to 2MM
                    files = _listFiles (folder, pattern, recursive).Take (2000000).ToList ();
                    _setStatus (true);
                    break;
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    // disconnect on error
                    Close ();
                    // delay in case of network error
                    System.Threading.Thread.Sleep (Details.RetryWaitMs);
                }
            }

            // sanity check
            if (files == null)
                yield break;

            _setStatus (true);
            // download files
            foreach (var f in files)
            {
                yield return new StreamTransferInfo
                {
                    FileName = f.FullName,
                    FileStream = client.OpenRead (f.FullName)
                };
            }
        }

        public IEnumerable<StreamTransferInfo> GetFileStreams ()
        {
            if (Details.HasWildCardSearch)
                return GetFileStreams (Details.BasePath, Details.FullPath.TrimStart ('/'), !Details.SearchTopDirectoryOnly);
            else
                return new StreamTransferInfo[] { GetFileStream (Details.FullPath) };
        }

        public FileTransferInfo GetFile (string file, string outputDirectory, bool deleteOnSuccess)
        {
            outputDirectory = outputDirectory.Replace ('\\', '/');
            if (!outputDirectory.EndsWith ("/"))
                outputDirectory += "/";
            FileTransferHelpers.CreateDirectory (outputDirectory);

            // download files
            var f = GetFileStream (file);

            string newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
            FileTransferHelpers.DeleteFile (newFile);

            try
            {
                using (var output = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete, FileServiceConnectionInfo.DefaultWriteBufferSize))
                {
                    f.FileStream.CopyTo (output, FileServiceConnectionInfo.DefaultWriteBufferSize >> 2);
                }

                // check if we must remove file
                if (deleteOnSuccess)
                {
                    FileTransferHelpers.DeleteFile (f.FileName);
                }

                _setStatus (true);
            }
            catch (Exception ex)
            {
                _setStatus (ex);
                FileTransferHelpers.DeleteFile (newFile);
                newFile = null;
            }
            finally
            {
                f.FileStream.Close ();
            }

            // check if file was downloaded
            if (newFile != null)
            {
                var info = new System.IO.FileInfo (newFile);
                if (info.Exists)
                    return new FileTransferInfo (newFile, info.Length, info.CreationTime, info.LastWriteTime);
            }
            return null;
        }

        public string GetFileAsText (string file)
        {
            using (var reader = new StreamReader (GetFileStream (file).FileStream, FileTransferHelpers.TryGetEncoding (Details.Get ("encoding", "ISO-8859-1")), true))
                return reader.ReadToEnd ();
        }

        public IEnumerable<FileTransferInfo> GetFiles (string folder, string fileMask, bool recursive, string outputDirectory, bool deleteOnSuccess)
        {
            outputDirectory = outputDirectory.Replace ('\\', '/');
            if (!outputDirectory.EndsWith ("/"))
                outputDirectory += "/";
            FileTransferHelpers.CreateDirectory (outputDirectory);

            // download files
            foreach (var f in GetFileStreams (folder, fileMask, recursive))
            {
                string newFile = null;                
                
                // download file
                try
                { 
                    newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
                    FileTransferHelpers.DeleteFile (newFile);

                    using (var file = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete, FileServiceConnectionInfo.DefaultWriteBufferSize))
                    {
                        f.FileStream.CopyTo (file, FileServiceConnectionInfo.DefaultWriteBufferSize >> 2);
                    }

                    f.FileStream.Close ();
                    f.FileStream = null;

                    // check if we must remove file
                    if (deleteOnSuccess && System.IO.File.Exists (newFile))
                    {   
                        client.DeleteFile (f.FileName);
                    }

                    _setStatus (true);
                    break;
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    Close ();
                    FileTransferHelpers.DeleteFile (newFile);
                    newFile = null;
                    // delay in case of network error
                    System.Threading.Thread.Sleep (Details.RetryWaitMs);
                    // try to reopen
                    Open ();
                }
                finally
                {
                    if (f.FileStream != null)
                        f.FileStream.Close ();
                }

                // check if file was downloaded
                if (newFile != null)
                {
                    var info = new System.IO.FileInfo (newFile);
                    if (info.Exists)
                        yield return new FileTransferInfo (newFile, info.Length, info.CreationTime, info.LastWriteTime);
                }
            }
        }

        public IEnumerable<FileTransferInfo> GetFiles (string outputDirectory, bool deleteOnSuccess)
        {
            if (Details.HasWildCardSearch)
                return GetFiles (Details.BasePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly, outputDirectory, deleteOnSuccess);
            else
                return new FileTransferInfo[] { GetFile (Details.FullPath, outputDirectory, deleteOnSuccess) };
        }

        public bool RemoveFiles (IEnumerable<string> files)
        {
            var enumerator = files.GetEnumerator ();            
            string file = null;
            for (int i = 0; i < Details.RetryCount; i++)
            {
                // try to open
                if (!Open ())
                    continue;
                     
                try
                {
                    if (file == null)
                    {
                        enumerator.MoveNext ();                                                
                    }

                    do
                    {
                        file = PreparePath (enumerator.Current);
                        client.DeleteFile (file);               
                    }
                    while (enumerator.MoveNext ());
                    _setStatus (true);
                    break;
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    // disconnect on error
                    Close ();
                    // delay in case of network error
                    System.Threading.Thread.Sleep (Details.RetryWaitMs);
                }
            }

            return Status;
        }

        public bool RemoveFile (string file)
        {
            _setStatus (false);
            for (int i = 0; i < Details.RetryCount; i++)
            {
                // try to open
                if (!Open ())
                    continue;

                try
                {
                    file = PreparePath (file);
                    client.DeleteFile (file);
                    _setStatus (true);
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    // disconnect on error
                    Close ();
                    // delay in case of network error
                    System.Threading.Thread.Sleep (Details.RetryWaitMs);
                }
            }
            return Status;
        }

        public bool SendFile (string localFilename)
        {
            return SendFile (localFilename, System.IO.Path.Combine (Details.BasePath, System.IO.Path.GetFileName (localFilename)));
        }

        public bool SendFile (string localFilename, string destFilename)
        {
            using (var file = new FileStream (localFilename, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, FileServiceConnectionInfo.DefaultReadBufferSize))
            {
                return SendFile (file, destFilename, false);
            }
        }

        public bool SendFile (Stream localFile, string destFullPath, bool closeInputStream)
        {
            // try to upload file
            for (int i = 0; i < Details.RetryCount; i++)
            {
                // try to open
                if (!Open ())
                    return false;
                try
                {
                    // check folder existance
                    CreateDirectory (PreparePath (FileTransferHelpers.SplitByLastPathPart (destFullPath).Item1));

                    // upload
                    using (Stream ostream = client.OpenWrite (PreparePath (destFullPath)))
                    {
                        using (var file = localFile)
                        {
                            file.CopyTo (ostream, FileServiceConnectionInfo.DefaultWriteBufferSize >> 2);
                        }
                    }                    
                    _setStatus (true);
                    return true;
                }
                catch (Exception ex)
                {
                    _setStatus (ex);
                    // disconnect on error
                    Close ();
                }
                finally
                {
                    if (closeInputStream)
                        localFile.Close();
                }
            }
            return false;
        }

        public bool MoveFile (string localFilename, string destFilename)
        {
            _setStatus (false, "Operation not supported");
            return Status;
        }

        public Stream OpenWrite ()
        {
            return OpenWrite (Details.FullPath);
        }

        public Stream OpenWrite (string destFullPath)
        {            
            // check folder existance
            CreateDirectory (PreparePath (FileTransferHelpers.SplitByLastPathPart (destFullPath).Item1));
            // upload
            return client.OpenWrite (PreparePath (destFullPath));
        }
    }
}