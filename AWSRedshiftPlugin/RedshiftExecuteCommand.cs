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
        public enum LoadTypes { Incremental }

        public string GetDescription ()
        {
            return "Redshift Execute Command";   
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

        public bool Execute (ISessionContext context)
        {
            var logger = context.GetLogger ();
            var options = context.Options;
            AWSS3Helper s3 = null;

            try
            {
                var loadScript = options.Get ("sqlScriptPath", "");                
                var customCSharpScriptPath = options.Get ("customCSharpScriptPath", "");                

                if ((!String.IsNullOrWhiteSpace (loadScript)) && (!String.IsNullOrWhiteSpace (customCSharpScriptPath)))
                {
                    throw new Exception ("No action configured");
                }

                // prepare paths
                var parsedLoadScript = FileTransferDetails.ParseSearchPath (loadScript);                
                var parsedCustomCSharpScriptPath = FileTransferDetails.ParseSearchPath (customCSharpScriptPath);

                // open s3 connection
                s3 = new AWSS3Helper (options.Get ("awsAccessKey", ""), options.Get ("awsSecretAccessKey", ""), parsedLoadScript.BucketName, Amazon.RegionEndpoint.USEast1, true);

                // load sql script
                string sqlScript = null;
                if (!String.IsNullOrWhiteSpace (loadScript))
                {
                    sqlScript = s3.ReadFileAsText (parsedLoadScript.FilePath, true);
                }

                // generate code
                IAWSRedshiftPluginDynamicScript customCode = null;
                if (!String.IsNullOrWhiteSpace (customCSharpScriptPath))
                {
                    // load custom code
                    var csharpScript = s3.ReadFileAsText (parsedCustomCSharpScriptPath.FilePath, true);

                    var evaluator = ScriptEvaluator.CompileAndCreateModel (csharpScript);
                    if (evaluator.HasError || evaluator.Model == null)
                        throw new Exception ("Script compilation error. " + (evaluator.Message ?? "<empty>"));
                    customCode = evaluator.Model;
                }

                // execute commands
                using (var conn = new Npgsql.NpgsqlConnection (RedshiftHelper.GetConnectionString (context)))
                {
                    conn.Open ();

                    if (customCode != null)
                    {
                        logger.Debug ("Custom csharp code Initialize");

                        customCode.Initialize (conn, s3, context);

                        logger.Debug ("Custom csharp code BeforeExecution");
                        customCode.BeforeExecution ();

                        logger.Debug ("Custom csharp code PrepareSqlCOPYCommand");
                        if (!String.IsNullOrEmpty (sqlScript))
                            sqlScript = customCode.PrepareSqlCOPYCommand (sqlScript);
                    }
                                        
                    if (!String.IsNullOrEmpty (sqlScript))
                    {
                        logger.Debug ("SQL command start");

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
                            logger.Info ("SQL command executed, but is still running (connection timeout)...");
                        }

                        logger.Debug ("SQL command end");
                    }                    

                    if (customCode != null)
                    {
                        logger.Debug ("Custom csharp code AfterExecution");
                        customCode.AfterExecution ();
                    }
                }
                logger.Success ("Done");
            }
            catch (Exception ex)
            {
                context.Error = ex.Message;
                logger.Error (ex);
                return false;
            }

            return true;
        }

    }
}
