using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Renci.SshNet;
using BigDataPipeline.Interfaces;

namespace FileListenerPlugin
{
    public class SFTPTransfer : IFileTransfer 
    {
        SftpClient client;

        static string[] serviceSchemes = new[] { "sftp", "ssh" };

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
                info.Port = 22;
            }

            info.FullPath = PreparePath (info.FullPath);
            info.BasePath = PreparePath (info.BasePath);

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
            // check for empty login/password
            if (String.IsNullOrEmpty (Details.Login) && String.IsNullOrEmpty (Details.Password))
            {
                Details.Login = Details.Get ("sftpLogin", Details.Login);
                Details.Password = Details.Get ("sftpPassword", Details.Password);
            }

            for (int i = 0; i < Details.RetryCount; i++)
            {
                try
                {
                    // set default ssh port if not is set
                    if (Details.Port <= 0)
                        Details.Port = 22;

                    // check for ssh key files
                    string[] SshKeyFiles = Details.Get<string[]>("sshKeyFiles", null);
                    if (SshKeyFiles == null && Details.Get<string> ("sshKeyFiles", null) != null)
                        SshKeyFiles = Details.Get<string> ("sshKeyFiles", null).Split (new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries);

                    // create client and try to connect
                    // if we are retrying, and a keyfile was set, try to use the password instead 
                    if (i % 2 == 1 && !String.IsNullOrEmpty (Details.Password) && SshKeyFiles != null && SshKeyFiles.Length > 0)
                        client = new SftpClient (Details.Host, Details.Port, Details.Login, Details.Password);
                    // connect with Private key file
                    else if (SshKeyFiles != null && SshKeyFiles.Length > 0)
                        client = new SftpClient (Details.Host, Details.Port, Details.Login, SshKeyFiles.Select (k =>
                        {
                            var d = k.Split(':');
                            if (d.Length > 1)
                                return new PrivateKeyFile (d[0], d[1]);
                            return new PrivateKeyFile (k);
                        }).ToArray ());
                    // connect with login/password
                    else
                        client = new SftpClient (Details.Host, Details.Port, Details.Login, Details.Password);


                    client.BufferSize = 1024 * 1024;
                    client.OperationTimeout = TimeSpan.FromHours (1);

                    client.Connect ();

                    // go to dir                    
                    client.ChangeDirectory (Details.BasePath);

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

        private void CreateDirectory (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                return;
            folder = PreparePath (folder);
            if (!client.Exists (folder))
            {
                var folders = folder.Trim ('/').Split ('/');
                var current = "/";
                foreach (var f in folders)
                {
                    current += f + "/";
                    if (!client.Exists (current))
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

        private IEnumerable<Renci.SshNet.Sftp.SftpFile> _listFiles (string folder, System.Text.RegularExpressions.Regex pattern, bool recursive)
        {
            _setStatus (true);

            if (!Open ())
                yield break;

            folder = PreparePath (folder);

            foreach (var f in client.ListDirectory (folder))
                if (f.IsDirectory && recursive && f.FullName.StartsWith (folder) && !f.FullName.EndsWith ("."))
                    foreach (var n in _listFiles (f.FullName, pattern, recursive))
                        yield return n;
                else if (f.IsRegularFile)
                    if (pattern == null || pattern.IsMatch (System.IO.Path.GetFileName (f.FullName)))
                        yield return f;
        }

        public IEnumerable<FileTransferInfo> ListFiles ()
        {
            return ListFiles (Details.BasePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, bool recursive)
        {
            return _listFiles (folder, null, recursive).Select (i => new FileTransferInfo (i.FullName, i.Length, i.LastWriteTime, i.LastWriteTime));
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, string fileMask, bool recursive)
        {
            var pattern = String.IsNullOrEmpty (fileMask) ? null : new System.Text.RegularExpressions.Regex (FileTransferHelpers.WildcardToRegex (fileMask), System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
            return _listFiles (folder, pattern, recursive).Select (i => new FileTransferInfo (i.FullName, i.Length, i.LastWriteTime, i.LastWriteTime));
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
            List<Renci.SshNet.Sftp.SftpFile> files = null;
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
                return GetFileStreams (Details.BasePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
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
                using (var output = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileServiceConnectionInfo.DefaultWriteBufferSize))
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

                    using (var file = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileServiceConnectionInfo.DefaultWriteBufferSize))
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
                    CreateDirectory (FileTransferHelpers.SplitByLastPathPart (destFullPath).Item1);

                    client.UploadFile (localFile, PreparePath(destFullPath), true);
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
                        localFile.Close ();
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