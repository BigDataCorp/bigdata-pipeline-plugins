using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace AWSRedshiftPlugin
{
    public class RedshiftExecuteCommand : IActionModule
    {
        string _lastError;

        public enum LoadTypes { Incremental, }

        public string Name { get; private set; }

        public string Description { get; private set; }

        public RedshiftExecuteCommand ()
        {
            Name = "RedshiftExecuteCommand";
            Description = "Redshift Execute Command";
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
            yield return new PluginParameterDetails ("sqlScriptPath", typeof (string), "[optional] sql script path", true);

            yield return new PluginParameterDetails ("customCSharpScriptPath", typeof (string), "[optional] s3 customCSharpScriptPath", true);
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
            try
            {
                var loadScript = _options.Get ("sqlScriptPath", "");                
                var customCSharpScriptPath = _options.Get ("customCSharpScriptPath", "");                

                if ((!String.IsNullOrWhiteSpace (loadScript)) && (!String.IsNullOrWhiteSpace (customCSharpScriptPath)))
                {
                    throw new Exception ("No action configured");
                }

                // prepare paths
                var parsedLoadScript = FileTransferDetails.ParseSearchPath (loadScript);                
                var parsedCustomCSharpScriptPath = FileTransferDetails.ParseSearchPath (customCSharpScriptPath);

                // open s3 connection
                _s3 = new AWSS3Helper (_options.Get ("awsAccessKey", ""), _options.Get ("awsSecretAccessKey", ""), parsedLoadScript.BucketName, Amazon.RegionEndpoint.USEast1, true);

                // load sql script
                string sqlScript = null;
                if (!String.IsNullOrWhiteSpace (loadScript))
                {
                    sqlScript = _s3.ReadFileAsText (parsedLoadScript.FilePath, true);
                }

                // generate code
                IAWSRedshiftPluginDynamicScript customCode = null;
                if (!String.IsNullOrWhiteSpace (customCSharpScriptPath))
                {
                    // load custom code
                    var csharpScript = _s3.ReadFileAsText (parsedCustomCSharpScriptPath.FilePath, true);

                    var evaluator = ScriptEvaluator.CompileAndCreateModel (csharpScript);
                    if (evaluator.HasError || evaluator.Model == null)
                        throw new Exception ("Script compilation error. " + (evaluator.Message ?? "<empty>"));
                    customCode = evaluator.Model;
                }

                // execute commands
                using (var conn = new Npgsql.NpgsqlConnection (RedshiftHelper.GetConnectionString (_options)))
                {
                    conn.Open ();

                    if (customCode != null)
                    {
                        _logger.Log ("Custom csharp code Initialize");

                        customCode.Initialize (conn, _s3, _logger, _options);

                        _logger.Log ("Custom csharp code BeforeExecution");
                        customCode.BeforeExecution ();

                        _logger.Log ("Custom csharp code PrepareSqlCOPYCommand");
                        if (!String.IsNullOrEmpty (sqlScript))
                            sqlScript = customCode.PrepareSqlCOPYCommand (sqlScript);
                    }
                                        
                    if (!String.IsNullOrEmpty (sqlScript))
                    {
                        _logger.Log ("SQL command start");

                        try
                        {
                            conn.Execute (sqlScript);
                        }
                        catch (Exception ex)
                        {
                            // do nothing in case of timeout... some operations may take a while to complete...
                            if (ex.Message.IndexOf ("timeout", StringComparison.OrdinalIgnoreCase) < 0)
                            {                                
                                throw ex;
                            }
                            _logger.Log ("SQL command executed, but is still running (connection timeout)...");
                        }

                        _logger.Log ("SQL command end");
                    }                    

                    if (customCode != null)
                    {
                        _logger.Log ("Custom csharp code AfterExecution");
                        customCode.AfterExecution ();
                    }
                }
                _logger.Success ("Done");
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _logger.Error ("[Error] " + _lastError);                
                return false;
            }

            return true;
        }

    }
}
