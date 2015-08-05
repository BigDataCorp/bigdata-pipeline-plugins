using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace AWSRedshiftPlugin
{
    public class RedshiftManagedLoad : IActionModule
    {
        public string GetDescription ()
        { 
            return "Redshift Managed Load";
        }

        public IEnumerable<ModuleParameterDetails> GetParameterDetails ()
        {
            yield return new ModuleParameterDetails ("awsAccessKey", typeof (string), "AWS Access key for S3 usage");
            yield return new ModuleParameterDetails ("awsSecretAccessKey", typeof (string), "AWS Secret Access key for S3 usage");
            yield return new ModuleParameterDetails ("awsRedshiftPort", typeof (int), "AWS Redshift");
            yield return new ModuleParameterDetails ("awsRedshiftHost", typeof (string), "AWS Redshift");
            yield return new ModuleParameterDetails ("awsRedshiftDatabase", typeof (string), "AWS Redshift");
            yield return new ModuleParameterDetails ("awsRedshiftLogin", typeof (string), "AWS Redshift");
            yield return new ModuleParameterDetails ("awsRedshiftPassword", typeof (string), "AWS Redshift");
            yield return new ModuleParameterDetails ("inputSearchPath", typeof (string), "search path for the input files", true);
            yield return new ModuleParameterDetails ("backupLocation", typeof (string), "folder location to copy backup files", true);
            yield return new ModuleParameterDetails ("sqlScriptPath", typeof (string), "sql script path", true);
            yield return new ModuleParameterDetails ("errorLocation", typeof (string), "s3 errorLocation", true);
        }

        public bool Execute (ISessionContext context)
        {
            var logger = context.GetLogger ();
            var options = context.Options;
            AWSS3Helper s3 = null;

            List<string> files = null;
            FileTransferDetails parsedErrorLocation = null;
            try
            {
                var inputSearchPath = options.Get ("inputSearchPath", "");
                if (String.IsNullOrEmpty (inputSearchPath))
                    throw new ArgumentNullException ("inputSearchPath");

                var backupLocation = options.Get ("backupLocation", "");
                if (String.IsNullOrEmpty (backupLocation))
                    throw new ArgumentNullException ("backupLocation");

                var loadScript  = options.Get ("sqlScriptPath", "");
                if (String.IsNullOrEmpty (loadScript))
                    throw new ArgumentNullException ("sqlScriptPath");

                var errorLocation  = options.Get ("errorLocation", "");
                if (String.IsNullOrEmpty (errorLocation))
                    throw new ArgumentNullException ("errorLocation");

                // prepare paths
                var parsedInput = FileTransferDetails.ParseSearchPath (inputSearchPath);
                var parsedLoadScript = FileTransferDetails.ParseSearchPath (loadScript);
                var parsedBackupLocation = FileTransferDetails.ParseSearchPath (backupLocation);
                parsedErrorLocation = FileTransferDetails.ParseSearchPath (errorLocation);
                
                // open s3 connection
                s3 = new AWSS3Helper (options.Get ("awsAccessKey", ""), options.Get ("awsSecretAccessKey", ""), parsedInput.BucketName, Amazon.RegionEndpoint.USEast1, true);
                
                // 1. check if there is any new file                
                files = GetFilesFromS3 (s3, parsedInput).Where (f => !f.EndsWith ("/")).ToList ();
                
                if (files.Any ())
                {
                    logger.Info ("Files found: " + files.Count);

                    var sqlScript = s3.ReadFileAsText (parsedLoadScript.FilePath, false);                    

                    if (String.IsNullOrEmpty (sqlScript))
                        throw new Exception ("Invalid sql script: " + parsedLoadScript.FilePath);
                   
                    // Create a PostgeSQL connection string.
                    var connectionString = RedshiftHelper.GetConnectionString (context);

                    ExecuteRedshiftLoad (connectionString, logger, sqlScript, files, parsedInput);
                    logger.Debug ("Moving files to backup folder");

                    // move files
                    // TODO: what happens if move fails?
                    foreach (var f in files)
                    {
                        var destName = System.IO.Path.Combine (parsedBackupLocation.FilePath, System.IO.Path.GetFileName (f));
                        if (s3.MoveFile (f, destName, false))
                        {
                            System.Threading.Thread.Sleep (250);
                            if (s3.MoveFile (f, destName, false))
                                logger.Error (String.Format ("Error moving file {0}, {1}", f, s3.LastError));
                        }
                    }
                    logger.Success ("Done");
                    return true;
                }
                else
                {
                    logger.Debug ("No file found");
                    return false;
                }
            }
            catch (Exception ex)
            {                
                context.Error = ex.Message;
                logger.Error (ex);
                try
                {
                    if (files != null && s3 != null)
                    // move files
                    foreach (var f in files)
                    {
                        var destName = System.IO.Path.Combine (parsedErrorLocation.FilePath, System.IO.Path.GetFileName (f));
                        s3.MoveFile (f, destName, false);
                    }
                } catch {}
                return false;
            }

            return true;
        }

        private void ExecuteRedshiftLoad (string connectionString, IActionLogger logger, string script, List<string> files, FileTransferDetails filesDetails)
        {
            var dtStart = DateTime.UtcNow;

            // prepare files full name
            HashSet<string> fileMap = new HashSet<string> (files
                .Select (f => System.IO.Path.Combine ("s3://", filesDetails.BucketName, f.Trim ()).Replace ('\\', '/')), 
                StringComparer.OrdinalIgnoreCase);

            // execute redshift load.
            logger.Debug ("SQL Script start");

            RedshiftHelper.RunLoad (connectionString, script);

            logger.Debug ("SQL Script end");

            // check in current load operations table, which files are being processed
            // keep doing that, until all files are finished!
            int num_files;
            do
            {
                num_files = 0;
                foreach (var i in stv_load_state.Read (connectionString))
                {
                    if (fileMap.Contains (i.current_file))
                        num_files++;
                }                
                // wait a while
                if (num_files > 0)
                {
                    // sanity check... 2 hours to run a load operation is too long...
                    if ((DateTime.UtcNow - dtStart) > TimeSpan.FromHours (3))
                        break;
                    // wait a while
                    System.Threading.Thread.Sleep (10000);
                }
            }
            while (num_files > 0);

            // check load errors table
            foreach (var i in stl_load_errors.Read (connectionString, dtStart))
            {
                if (fileMap.Contains (i.filename.Trim ()))
                    throw new Exception ("Load error detected <stl_load_errors>: " + Newtonsoft.Json.JsonConvert.SerializeObject (i));
            }

            // check commited files table
            var filesLoaded = new HashSet<string> (stl_load_commits.ReadCommitedFiles (connectionString, dtStart).Select (f => f.Trim ()), StringComparer.OrdinalIgnoreCase);
            foreach (var f in fileMap)
            {
                if (!filesLoaded.Contains (f))
                    throw new Exception ("Load error; file not commited <stl_load_commits>: " + f);
            }

            logger.Debug ("Files loaded");
        }
        
        private IEnumerable<string> GetFilesFromS3 (AWSS3Helper s3, FileTransferDetails parsed)
        {            
            // connect to s3

            if (parsed.UseWildCardSearch)
            {
                var rgx = new System.Text.RegularExpressions.Regex (parsed.SearchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
                foreach (var f in s3.GetFileList (parsed.FilePath, true, true))
                {
                    if (rgx.IsMatch (f))
                        yield return f;
                }
            }
            else
            {
                foreach (var f in s3.GetFileList (parsed.FilePath, true, true))
                    yield return f;
            }
        }

    }
}
