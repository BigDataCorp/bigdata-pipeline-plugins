using System;
using System.Linq;
using System.IO;
using System.Net;
using System.Net.FtpClient;
using System.Text.RegularExpressions;
using System.Collections.Generic;

namespace FileListenerPlugin
{
    public class FTPTransfer : IFileTransfer 
    {
        FtpClient client;        

        public FileTransferDetails Details { get; private set; }

        public FileTranferStatus Status { get; private set; }

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
            Status = status ? FileTranferStatus.Success : FileTranferStatus.Error;
            LastError = message;
        }
                
        public bool Open (FileTransferDetails details)
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
                Details.Port = Details.Location == FileLocation.FTPS ? 990 : 21;

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
            var list = new List<Tuple<FileLocation, FtpDataConnectionType>> ()
                {
                    new Tuple<FileLocation, FtpDataConnectionType> (FileLocation.FTP, FtpDataConnectionType.AutoPassive),
                    new Tuple<FileLocation, FtpDataConnectionType> (FileLocation.FTP, FtpDataConnectionType.AutoActive),
                    new Tuple<FileLocation, FtpDataConnectionType> (FileLocation.FTPS, FtpDataConnectionType.AutoPassive),
                    new Tuple<FileLocation, FtpDataConnectionType> (FileLocation.FTPS, FtpDataConnectionType.AutoActive),
                    new Tuple<FileLocation, FtpDataConnectionType> (FileLocation.FTPES, FtpDataConnectionType.AutoPassive),
                    new Tuple<FileLocation, FtpDataConnectionType> (FileLocation.FTPES, FtpDataConnectionType.AutoActive)
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

                    client.Host = Details.Server;
                    client.Port = Details.Port;
                    client.Credentials = new NetworkCredential (Details.Login, Details.Password);
                    client.ValidateCertificate += new FtpSslValidation (OnValidateCertificate);
                    client.SocketKeepAlive = true;
                    client.ReadTimeout = 30000;

                    client.EncryptionMode = mode.Item1 == FileLocation.FTP ? FtpEncryptionMode.None : mode.Item1 == FileLocation.FTPS ? FtpEncryptionMode.Implicit : FtpEncryptionMode.Explicit;

                    client.Connect ();

                    // go to dir
                    if (!String.IsNullOrEmpty (Details.FilePath))
                    {
                        CreateDirectory (Details.FilePath);
                        client.SetWorkingDirectory (Details.FilePath);
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
            if (!client.DirectoryExists (folder))
            {
                var folders = folder.Trim ('/').Split ('/');
                var current = "/";
                foreach (var f in folders)
                {
                    current += f + "/";
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
                    if (pattern == null || pattern.IsMatch (System.IO.Path.GetFileName (f.FullName)))
                        yield return f;
        }

        public IEnumerable<FileTransferInfo> ListFiles ()
        {
            return ListFiles (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, bool recursive)
        { 
            return _listFiles (folder, null, recursive).Select (i => new FileTransferInfo (i.FullName, i.Size, i.Created, i.Modified));
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, string fileMask, bool recursive)
        {
            var pattern = String.IsNullOrEmpty (fileMask) ? null : new System.Text.RegularExpressions.Regex (FileTransferDetails.WildcardToRegex (fileMask), System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
            return _listFiles (folder, pattern, recursive).Select (i => new FileTransferInfo (i.FullName, i.Size, i.Created, i.Modified));
        }

        public StreamTransfer GetFileStream (string file)
        {
            _setStatus (true);
            // download files
            return new StreamTransfer
            {
                FileName = file,
                FileStream = client.OpenRead (file)
            };
        }

        public IEnumerable<StreamTransfer> GetFileStreams (string folder, string fileMask, bool recursive)
        {
            var pattern = String.IsNullOrEmpty (fileMask) ? null : new System.Text.RegularExpressions.Regex (FileTransferDetails.WildcardToRegex (fileMask), System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);

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
                yield return new StreamTransfer
                {
                    FileName = f.FullName,
                    FileStream = client.OpenRead (f.FullName)
                };
            }
        }

        public IEnumerable<StreamTransfer> GetFileStreams ()
        {
            return GetFileStreams (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
        }

        public FileTransferInfo GetFile (string file, string outputDirectory, bool deleteOnSuccess)
        {
            outputDirectory = outputDirectory.Replace ('\\', '/');
            if (!outputDirectory.EndsWith ("/"))
                outputDirectory += "/";
            FileTransferDetails.CreateDirectory (outputDirectory);

            // download files
            var f = GetFileStream (file);

            string newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
            FileTransferDetails.DeleteFile (newFile);

            try
            {
                using (var output = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultWriteBufferSize))
                {
                    f.FileStream.CopyTo (output, FileTransferDetails.DefaultWriteBufferSize >> 2);
                }

                // check if we must remove file
                if (deleteOnSuccess)
                {
                    FileTransferDetails.DeleteFile (f.FileName);
                }

                _setStatus (true);
            }
            catch (Exception ex)
            {
                _setStatus (ex);
                FileTransferDetails.DeleteFile (newFile);
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

        public IEnumerable<FileTransferInfo> GetFiles (string folder, string fileMask, bool recursive, string outputDirectory, bool deleteOnSuccess)
        {
            outputDirectory = outputDirectory.Replace ('\\', '/');
            if (!outputDirectory.EndsWith ("/"))
                outputDirectory += "/";
            FileTransferDetails.CreateDirectory (outputDirectory);

            // download files
            foreach (var f in GetFileStreams (folder, fileMask, recursive))
            {
                string newFile = null;                
                
                // download file
                try
                { 
                    newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
                    FileTransferDetails.DeleteFile (newFile);

                    using (var file = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultWriteBufferSize))
                    {
                        f.FileStream.CopyTo (file, FileTransferDetails.DefaultWriteBufferSize >> 2);
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
                    FileTransferDetails.DeleteFile (newFile);
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
            return GetFiles (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly, outputDirectory, deleteOnSuccess);
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

            return Status == FileTranferStatus.Success;
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
            return Status == FileTranferStatus.Success;
        }

        public bool SendFile (string localFilename)
        {
            return SendFile (localFilename, System.IO.Path.Combine (Details.FilePath, System.IO.Path.GetFileName (localFilename)));
        }

        public bool SendFile (string localFilename, string destFilename)
        {
            using (var file = new FileStream (localFilename, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, FileTransferDetails.DefaultReadBufferSize))
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
                    CreateDirectory (PreparePath (System.IO.Path.GetDirectoryName (destFullPath)));

                    // upload
                    using (Stream ostream = client.OpenWrite (PreparePath (destFullPath)))
                    {
                        using (var file = localFile)
                        {
                            file.CopyTo (ostream, FileTransferDetails.DefaultWriteBufferSize >> 2);
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
    }
}