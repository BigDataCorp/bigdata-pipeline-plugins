using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileListenerPlugin
{
    public class FileSystemTransfer : IFileTransfer
    {

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
            return true;
        }

        private void CreateDirectory (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                return;
            FileTransferHelpers.CreateDirectory (folder);
        }

        private string PreparePath (string folder)
        {
            if (String.IsNullOrEmpty (folder))
                folder = "/";           
            return folder;
        }

        public bool IsOpened ()
        {
            return true;
        }

        public void Dispose ()
        {            
        }

        private IEnumerable<FileTransferInfo> _listFiles (string folder, string pattern, bool recursive)
        {
            _setStatus (true);

            folder = PreparePath (folder);

            if (String.IsNullOrEmpty (pattern))
                pattern = "*";

            return System.IO.Directory.EnumerateFiles (folder, pattern, recursive ? System.IO.SearchOption.AllDirectories : System.IO.SearchOption.TopDirectoryOnly)
                       .Select (i => new FileInfo (i)).Select (i => new FileTransferInfo (i.FullName, i.Length, i.CreationTime, i.LastWriteTime));
        }

        public IEnumerable<FileTransferInfo> ListFiles ()
        {
            return ListFiles (Details.FilePath, Details.SearchPattern, !Details.SearchTopDirectoryOnly);
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, bool recursive)
        {
            return _listFiles (folder, null, recursive);
        }

        public IEnumerable<FileTransferInfo> ListFiles (string folder, string fileMask, bool recursive)
        {
            return _listFiles (folder, fileMask, recursive);
        }

        public StreamTransfer GetFileStream (string file)
        {
            _setStatus (true);
            // download files
            return new StreamTransfer
            {
                FileName = file,
                FileStream = new FileStream (file, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, FileTransferConnectionInfo.DefaultReadBufferSize)
            };
        }

        public IEnumerable<StreamTransfer> GetFileStreams (string folder, string fileMask, bool recursive)
        {
            _setStatus (true);
            // download files
            foreach (var f in _listFiles (folder, fileMask, recursive))
            {
                yield return new StreamTransfer
                {
                    FileName = f.FileName,
                    FileStream = new FileStream (f.FileName, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, FileTransferConnectionInfo.DefaultReadBufferSize)
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
                string newFile = System.IO.Path.Combine (outputDirectory, System.IO.Path.GetFileName (f.FileName));
                FileTransferHelpers.DeleteFile (newFile);

                try
                {
                    using (var file = new FileStream (newFile, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferConnectionInfo.DefaultWriteBufferSize))
                    {
                        f.FileStream.CopyTo (file, FileTransferConnectionInfo.DefaultWriteBufferSize >> 2);
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
            foreach (var f in files)
                FileTransferHelpers.DeleteFile (f);

            _setStatus (true);

            return Status == FileTranferStatus.Success;
        }

        public bool RemoveFile (string file)
        {
            FileTransferHelpers.DeleteFile (file);
            _setStatus (true);
            return Status == FileTranferStatus.Success;
        }

        public bool SendFile (string localFilename)
        {
            return SendFile (localFilename, System.IO.Path.Combine (Details.FilePath, System.IO.Path.GetFileName (localFilename)));
        }

        public bool SendFile (string localFilename, string destFilename)
        {
            using (var file = new FileStream (localFilename, FileMode.Open, FileAccess.Read, FileShare.Delete | FileShare.Read, 1024 * 1024))
            {
                return SendFile (file, destFilename, false);
            }
        }

        public bool SendFile (Stream localFile, string destFullPath, bool closeInputStream)
        {
            try
            {
                FileTransferHelpers.CreateDirectory (System.IO.Path.GetDirectoryName (destFullPath));

                using (var file = new FileStream (destFullPath, FileMode.Create, FileAccess.Write, FileShare.Delete | FileShare.Read, FileTransferConnectionInfo.DefaultWriteBufferSize))
                {
                    localFile.CopyTo (file, FileTransferConnectionInfo.DefaultWriteBufferSize >> 2);
                }
                _setStatus (true);
            }
            catch (Exception ex)
            {
                _setStatus (ex);
            }
            finally
            {
                if (closeInputStream)
                    localFile.Close ();
            }
            return Status == FileTranferStatus.Success;
        }
    }
}
