using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Renci.SshNet;

namespace FileListenerPlugin
{
    public class SFTPTransfer : IFileTransfer 
    {
        SftpClient client;        

        public FileTransferConnectionInfo Details { get; private set; }

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
                
        public bool Open (FileTransferConnectionInfo details)
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

                    // create client and tru to connect
                    // if we are retrying, and a keyfile was set, try to use the password instead 
                    if (i % 2 == 1 && !String.IsNullOrEmpty (Details.Password) && SshKeyFiles != null && SshKeyFiles.Length > 0)
                        client = new SftpClient (Details.Server, Details.Port, Details.Login, Details.Password);
                    // connect with Private key file
                    else if (SshKeyFiles != null && SshKeyFiles.Length > 0)
                        client = new SftpClient (Details.Server, Details.Port, Details.Login, SshKeyFiles.Select (k => new PrivateKeyFile (k)).ToArray ());
                    // connect with login/password
                    else
                        client = new SftpClient (Details.Server, Details.Port, Details.Login, Details.Password);

                    client.Connect ();

                    // go to dir
                    if (!String.IsNullOrEmpty (Details.FilePath))
                    {
                        Details.FilePath = PreparePath (Details.FilePath);
                        CreateDirectory (Details.FilePath);
                        client.ChangeDirectory (Details.FilePath);
                    }
                    else
                    {
                        Details.FilePath = PreparePath (Details.FilePath);
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
            return ListFiles (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
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
            FileTransferHelpers.CreateDirectory (outputDirectory);

            // download files
            var f = GetFileStream (file);

            string newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
            FileTransferHelpers.DeleteFile (newFile);

            try
            {
                using (var output = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferConnectionInfo.DefaultWriteBufferSize))
                {
                    f.FileStream.CopyTo (output, FileTransferConnectionInfo.DefaultWriteBufferSize >> 2);
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

                    using (var file = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferConnectionInfo.DefaultWriteBufferSize))
                    {
                        f.FileStream.CopyTo (file, FileTransferConnectionInfo.DefaultWriteBufferSize >> 2);
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
            using (var file = new FileStream (localFilename, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, FileTransferConnectionInfo.DefaultReadBufferSize))
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
                    CreateDirectory (System.IO.Path.GetDirectoryName (destFullPath));

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
    }
}