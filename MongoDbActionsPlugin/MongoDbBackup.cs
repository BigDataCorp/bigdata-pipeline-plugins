using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDbActionsPlugin
{
    public class MongoDbDatabaseCopy : IActionModule
    {
        string _lastError;

        public enum LoadTypes { Incremental }

        public string Name { get; private set; }

        public string Description { get; private set; }

        public MongoDbDatabaseCopy ()
        {
            Name = "MongoDbBackup";
            Description = "MongoDb Backup";
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
            yield return new PluginParameterDetails ("inputSearchPath", typeof (string), "search path for the input files");
            yield return new PluginParameterDetails ("backupLocation", typeof (string), "folder location to copy backup files");
            yield return new PluginParameterDetails ("sqlScriptPath", typeof (string), "sql script path");
            yield return new PluginParameterDetails ("errorLocation", typeof (string), "s3 errorLocation");
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

        public void SetParameters (Record options, ActionLogger logger)
        {
            _options = options;
            _logger = logger;
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

                // prepare paths
                var parsedInput = FileSearchDetails.ParseSearchPath (inputSearchPath);
                var parsedLoadScript = FileSearchDetails.ParseSearchPath (loadScript);
                var parsedBackupLocation = FileSearchDetails.ParseSearchPath (backupLocation);
                parsedErrorLocation = FileSearchDetails.ParseSearchPath (errorLocation);

                // open s3 connection
                _s3 = new AWSS3Helper (_options.Get ("awsAccessKey", ""), _options.Get ("awsSecretAccessKey", ""), parsedInput.BucketName, Amazon.RegionEndpoint.USEast1, true);

                // 1. check if there is any new file                
                files = GetFilesFromS3 (_s3, parsedInput).Where (f => !f.EndsWith ("/")).ToList ();

                if (files.Any ())
                {
                    _logger.Log ("Files found: " + files.Count);

                    var sqlScript = _s3.ReadFileAsText (parsedLoadScript.FilePath, true);

                    if (String.IsNullOrEmpty (sqlScript))
                        throw new Exception ("invalid sql script");

                    // Create a PostgeSQL connection string.
                    ExecuteRedshiftLoad (sqlScript, files, parsedInput);
                    _logger.Log ("Moving files to backup folder");

                    // move files
                    // TODO: what happens if move fails?
                    foreach (var f in files)
                    {
                        var destName = System.IO.Path.Combine (parsedBackupLocation.FilePath, System.IO.Path.GetFileName (f));
                        _s3.MoveFile (f, destName, false);
                    }
                    _logger.Success ("Done");
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _logger.Error ("[Error] " + _lastError);
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

        /// The copydb command copies a database from a remote host to the current host.
        /// 
        /// MongoDb copydb Command syntax:
        /// 
        /// { copydb: 1:
        /// fromhost: <hostname>,
        /// fromdb: <db>,
        /// todb: <db>,
        /// slaveOk: <bool>, [optional]
        /// username: <username>, [optional]
        /// nonce: <nonce>, [optional]
        /// key: <key> } [optional]
        

        public bool DatabaseBackup ()
        {
            var fromdb = _options.Get ("fromdb", "");
            if (String.IsNullOrEmpty (fromdb))
                throw new ArgumentNullException ("fromdb");

            var fromHostLogin = _options.Get ("fromHostLogin", "");
            if (String.IsNullOrEmpty (fromHostLogin))
                throw new ArgumentNullException ("fromHostLogin");

            var fromHostPassword = _options.Get ("fromHostPassword", "");
            if (String.IsNullOrEmpty (fromHostPassword))
                throw new ArgumentNullException ("fromHostPassword");

            var fromHostAddress = _options.Get ("fromHostAddress", "");
            if (String.IsNullOrEmpty (fromHostAddress))
                throw new ArgumentNullException ("fromHostAddress");


            var todb = _options.Get ("todb", "");
            if (String.IsNullOrEmpty (todb))
                throw new ArgumentNullException ("todb");

            var toHostLogin = _options.Get ("toHostLogin", "");
            if (String.IsNullOrEmpty (toHostLogin))
                throw new ArgumentNullException ("toHostLogin");

            var toHostPassword = _options.Get ("toHostPassword", "");
            if (String.IsNullOrEmpty (toHostPassword))
                throw new ArgumentNullException ("toHostPassword");

            var toHostAddress = _options.Get ("toHostAddress", "");
            if (String.IsNullOrEmpty (toHostAddress))
                throw new ArgumentNullException ("toHostAddress");




            

            var errorLocation  = _options.Get ("errorLocation", "");
            if (String.IsNullOrEmpty (errorLocation))
                throw new ArgumentNullException ("errorLocation");

            MongoDbContext.Configure(GlobalSettings.Mongo_UserName, GlobalSettings.Mongo_Password, GlobalSettings.Mongo_ServerPath);

            // check backup database name
            if (String.IsNullOrWhiteSpace (bkpDataBaseName))
            {
                // prepare backup database name
                int version = 1;
                bkpDataBaseName = GlobalSettings.Mongo_Database + "_bkp_" + DateTime.UtcNow.ToString ("yyyyMMdd") + "_";
                while (MongoDbContext.Server.DatabaseExists (bkpDataBaseName + version))
                {
                    version++;
                }
                bkpDataBaseName += version;
            }
            // sanity check
            if (bkpDataBaseName == GlobalSettings.Mongo_Database)
                return;
            // drop older database
            try
            {
                MongoDbContext.Server.DropDatabase (bkpDataBaseName);
            }
            catch (Exception ex) { LogManager.GetCurrentClassLogger ().Warn ("Drop Backup Database", ex); }

            // execute copy command
            // http://docs.mongodb.org/manual/reference/command/copydb/
            //
            var command = new CommandDocument (new BsonElement ("copydb", 1),
                //new BsonElement ("fromhost", MongoDbContext.Server.Primary.Address.Host + ':' + MongoDbContext.Server.Primary.Address.Port.ToString ()),                
                new BsonElement ("fromdb", GlobalSettings.Mongo_Database),
                new BsonElement ("todb", bkpDataBaseName),
                new BsonElement ("slaveOK", true));
            MongoDbContext.Server.GetDatabase ("admin").RunCommand (command);
        }


    }
}
