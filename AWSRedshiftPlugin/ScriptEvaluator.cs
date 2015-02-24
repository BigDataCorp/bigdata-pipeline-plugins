using BigDataPipeline.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace AWSRedshiftPlugin
{
    public class ScriptEvaluator
    {
        public string Script { get; set; }

        public bool Result { get; set; }

        public bool HasError { get; set; }

        public string Message { get; set; }

        public IAWSRedshiftPluginDynamicScript Model { get; set; }

        public ScriptEvaluator (string expression)
        {
            Script = expression;
            HasError = false;
            Result = false;
        }

        public static ScriptEvaluator CompileAndCreateModel (string expression)
        {
            return new ScriptEvaluator (expression).CompileAndCreateModel ();
        }

        public ScriptEvaluator CompileAndCreateModel ()
        { 
            Message = null;
            HasError = false;
            Result = false;

            var reportWriter = new System.IO.StringWriter ();

            try
            {
                var settings = new Mono.CSharp.CompilerSettings ();
                settings.GenerateDebugInfo = false;
                settings.LoadDefaultReferences = true;
                settings.Optimize = true;
                settings.WarningsAreErrors = false;                
                
                var reporter = new Mono.CSharp.ConsoleReportPrinter (reportWriter);

                var ctx = new Mono.CSharp.CompilerContext (settings, reporter);

                var scriptEngine = new Mono.CSharp.Evaluator (ctx);
                
                scriptEngine.ReferenceAssembly (this.GetType ().Assembly);
                scriptEngine.ReferenceAssembly (typeof (BigDataPipeline.Interfaces.Record).Assembly);
                scriptEngine.ReferenceAssembly (typeof (Newtonsoft.Json.JsonConvert).Assembly);

                scriptEngine.ReferenceAssembly (typeof (System.Data.Common.DbConnection).Assembly);
                scriptEngine.ReferenceAssembly (typeof (Dapper.SqlMapper).Assembly);
                scriptEngine.ReferenceAssembly (typeof (Npgsql.NpgsqlConnection).Assembly);
                scriptEngine.ReferenceAssembly (typeof (Amazon.S3.AmazonS3Client).Assembly);
                

                if (String.IsNullOrWhiteSpace (Script))
                    throw new ArgumentNullException ("Expression");

                
                if (!scriptEngine.Run (Script))
                    throw new Exception (reportWriter.ToString ());

                if (reporter.ErrorsCount > 0)
                {
                    throw new Exception (reportWriter.ToString ());                 
                }

                object model = scriptEngine.Evaluate ("new EvaluatorDynamicScript();");

                if (reporter.ErrorsCount > 0)
                {
                    throw new Exception (reportWriter.ToString ());                 
                }

                Model = model as IAWSRedshiftPluginDynamicScript;

                //Mono.CSharp.CompiledMethod method = scriptEngine.Compile ("myClass.Test1 ();");

                //if (reporter.ErrorsCount > 0)
                //{
                //    throw new Exception (reportWriter.ToString ());
                //}
                //if (method == null)
                //{
                //    throw new Exception ("script method not found");
                //}

                // execute method
                //object result = null;
                //method (ref result);
                
                //// check result
                //if (result is bool)
                //    Result = (bool)result;
                //else
                //    Result = false;
            }
            catch (Exception e)
            {                
                Message = e.Message;
                HasError = true;
            }
            return this;
        }
    }

    public interface IAWSRedshiftPluginDynamicScript
    {
        void Initialize (DbConnection connection, AWSS3Helper s3, ISessionContext context);

        void BeforeExecution ();

        string PrepareSqlCOPYCommand (string sql);

        void AfterExecution ();

    }
}