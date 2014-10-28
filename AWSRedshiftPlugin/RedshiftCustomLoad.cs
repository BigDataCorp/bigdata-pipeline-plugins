using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace AWSRedshiftPlugin
{
    public class RedshiftCustomLoad : IActionModule
    {
        string _lastError;

        public enum LoadTypes { Incremental, }

        public string Name { get; private set; }

        public string Description { get; private set; }

        public RedshiftCustomLoad ()
        {
            Name = "RedshiftCustomLoad";
            Description = "Redshift Custom Load";
        }

        public IEnumerable<PluginParameterDetails> GetParameterDetails ()
        {
            yield return new PluginParameterDetails ("awsAccessKey", typeof (string), "AWS Access key for S3 usage");
            yield return new PluginParameterDetails ("awsSecretAccessKey", typeof (string), "AWS Secret Access key for S3 usage");
            yield return new PluginParameterDetails ("awsRedshiftPort", typeof (int), "AWS Redshift");
            yield return new PluginParameterDetails ("awsRedshiftHost", typeof (string), "AWS Redshift");
            yield return new PluginParameterDetails ("awsRedshiftDatabase", typeof (string), "AWS Redshift");
            yield return new PluginParameterDetails ("awsRedshiftLogin", typeof (string), "AWS Redshift");
            yield return new PluginParameterDetails ("awsRedshiftPassword", typeof (string), "AWS Redshift");
            yield return new PluginParameterDetails ("inputSearchPath", typeof (string), "search path for the input files", true);
            yield return new PluginParameterDetails ("backupLocation", typeof (string), "folder location to copy backup files", true);
            yield return new PluginParameterDetails ("sqlScriptPath", typeof (string), "sql script path", true);
            yield return new PluginParameterDetails ("errorLocation", typeof (string), "s3 errorLocation", true);

            yield return new PluginParameterDetails ("customCSharpScriptPath", typeof (string), "s3 customCSharpScriptPath", true);
        }

        public string GetLastError ()
        {
            return _lastError;
        }

        public void CleanUp ()
        {

        }

        Record _options;
        ActionLogger _logger;
        AWSS3Helper _s3;
        ISessionContext _context;

        public void SetParameters (Record options, ISessionContext context)
        {
            _context = context;
            _options = options;
            _logger = _context.GetLogger ();
        }

        public bool Execute (params IEnumerable<Record>[] dataStreams)
        {
            _lastError = null;
            List<string> files = null;
            FileSearchDetails parsedErrorLocation = null;
            try
            {
                var inputSearchPath = _options.Get ("inputSearchPath", "");
                if (String.IsNullOrEmpty (inputSearchPath))
                    throw new ArgumentNullException ("inputSearchPath");

                var backupLocation = _options.Get ("backupLocation", "");
                if (String.IsNullOrEmpty (backupLocation))
                    throw new ArgumentNullException ("backupLocation");

                var loadScript  = _options.Get ("sqlScriptPath", "");
                if (String.IsNullOrEmpty (loadScript))
                    throw new ArgumentNullException ("sqlScriptPath");

                var errorLocation  = _options.Get ("errorLocation", "");
                if (String.IsNullOrEmpty (errorLocation))
                    throw new ArgumentNullException ("errorLocation");

                var customCSharpScriptPath  = _options.Get ("customCSharpScriptPath", "");
                if (String.IsNullOrEmpty (customCSharpScriptPath))
                    throw new ArgumentNullException ("customCSharpScriptPath");

                // prepare paths
                var parsedInput = FileSearchDetails.ParseSearchPath (inputSearchPath);
                var parsedLoadScript = FileSearchDetails.ParseSearchPath (loadScript);
                var parsedBackupLocation = FileSearchDetails.ParseSearchPath (backupLocation);
                parsedErrorLocation = FileSearchDetails.ParseSearchPath (errorLocation);
                var parsedCustomCSharpScriptPath = FileSearchDetails.ParseSearchPath (customCSharpScriptPath);

                // open s3 connection
                _s3 = new AWSS3Helper (_options.Get ("awsAccessKey", ""), _options.Get ("awsSecretAccessKey", ""), parsedInput.BucketName, Amazon.RegionEndpoint.USEast1, true);

                var csharpScript = _s3.ReadFileAsText (parsedCustomCSharpScriptPath.FilePath, true);

                // generate code
                var evaluator = ScriptEvaluator.CompileAndCreateModel (csharpScript);
                if (evaluator.HasError || evaluator.Model == null)
                    throw new Exception ("Script compilation error. " + (evaluator.Message ?? "<empty>"));

                // 1. check if there is any new file                
                files = GetFilesFromS3 (_s3, parsedInput).ToList ();

                if (files.Any ())
                {
                    _logger.Log ("Files found: " + files.Count);
                }
                else
                {
                    _logger.Debug ("No file found");
                    return false;
                }
                    
                foreach (var f in files)
                {
                    var sqlScript = _s3.ReadFileAsText (parsedLoadScript.FilePath, true);

                    if (String.IsNullOrEmpty (sqlScript))
                        throw new Exception ("invalid sql script");

                    using (var conn = new Npgsql.NpgsqlConnection (RedshiftHelper.GetConnectionString (_options)))
                    {
                        conn.Open ();
                        var fullFilename = System.IO.Path.Combine ("s3://", parsedInput.BucketName, f.Trim ()).Replace ('\\', '/');

                        _options.Set ("InputFilename", fullFilename);
                        evaluator.Model.Initialize (conn, _s3, _logger, _options);

                        evaluator.Model.BeforeExecution ();

                        sqlScript = evaluator.Model.PrepareSqlCOPYCommand (sqlScript);
                        
                        // Create a PostgeSQL connection string.
                        ExecuteRedshiftLoad (sqlScript, new List<string> () { f }, parsedInput);
                        _logger.Log ("Moving files to backup folder");

                        evaluator.Model.AfterExecution ();

                        // move files                        
                        var destName = System.IO.Path.Combine (parsedBackupLocation.FilePath, System.IO.Path.GetFileName (f));
                        _s3.MoveFile (f, destName, false);
                    }
                    _logger.Success ("Done");
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _logger.Error ("[ex] " + _lastError);
                try
                {
                    if (files != null && _s3 != null)
                        // move files
                        foreach (var f in files)
                        {
                            var destName = System.IO.Path.Combine (parsedErrorLocation.FilePath, System.IO.Path.GetFileName (f));
                            _s3.MoveFile (f, destName, false);
                        }
                }
                catch { }
                return false;
            }

            return true;
        }

        private void ExecuteRedshiftLoad (string script, List<string> files, FileSearchDetails filesDetails)
        {
            var dtStart = DateTime.UtcNow;

            // prepare files full name
            HashSet<string> fileMap = new HashSet<string> (files
                .Select (f => System.IO.Path.Combine ("s3://", filesDetails.BucketName, f.Trim ()).Replace ('\\', '/')),
                StringComparer.OrdinalIgnoreCase);

            // Create a PostgeSQL connection string.
            var connectionString = RedshiftHelper.GetConnectionString (_options);

            _logger.Log ("SQL Script start");

            RedshiftHelper.RunLoad (connectionString, script);

            _logger.Log ("SQL Script end");

            int num_files = files.Count;
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

            _logger.Log ("Files loaded");
        }
  
        private IEnumerable<string> GetFilesFromS3 (AWSS3Helper s3, FileSearchDetails parsed)
        {
            // get file from s3
            if (parsed.UseWildCardSearch)
            {
                var rgx = new System.Text.RegularExpressions.Regex (parsed.SearchPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
                foreach (var f in s3.GetFileList (parsed.FilePath, true, true).Where (f => !f.EndsWith ("/")))
                {
                    if (rgx.IsMatch (f))
                        yield return f;
                }
            }
            else
            {
                foreach (var f in s3.GetFileList (parsed.FilePath, true, true).Where (f => !f.EndsWith ("/")))
                    yield return f;
            }
        }
    }
}
