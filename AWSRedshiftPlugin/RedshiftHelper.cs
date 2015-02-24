using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace AWSRedshiftPlugin
{
    public class RedshiftHelper
    {
        public static void RunLoad (string connectionString, string script)
        {
            try
            {
                // Make a connection using the Npgsql provider.
                using (var conn = new Npgsql.NpgsqlConnection (connectionString))
                {
                    conn.Open ();
                    conn.Execute (script);
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.IndexOf ("timeout", StringComparison.OrdinalIgnoreCase) > 0)
                {
                    // do nothing, if the loaded files are large this operation
                    // may take a while to complete...
                }
                else
                {
                    if (ex.Message.IndexOf ("stl_load_errors", StringComparison.OrdinalIgnoreCase) > 0)
                        throw ex;
                    throw ex;
                }
            }
        }

        public static string GetConnectionString (BigDataPipeline.Interfaces.ISessionContext context)
        {
            var options = context.Options;
            // Create a PostgeSQL connection string.
            var csb = new Npgsql.NpgsqlConnectionStringBuilder ();
            csb.Host = options.Get ("awsRedshiftHost", "");
            csb.Port = options.Get ("awsRedshiftPort", 0);
            csb.Database = options.Get ("awsRedshiftDatabase", "");
            csb.UserName = options.Get ("awsRedshiftLogin", "");
            csb.Password = options.Get ("awsRedshiftPassword", "");
            csb.CommandTimeout = (int)TimeSpan.FromHours (2).TotalSeconds;
            csb.Timeout = 900; // 1024 seconds is the maximum allowed timeout for the Npgsql driver
            csb.Pooling = false;
            return csb.ToString ();
        }
    }


    // "select filename, colname, position, err_code, err_reason from stl_load_errors where starttime >= @dtStart order by starttime desc limit 1000;"
    public class stl_load_errors
    {
        public string filename { get; set; }
        public string colname { get; set; }
        public int position { get; set; }
        public int err_code { get; set; }
        public string err_reason { get; set; }

        public static IEnumerable<stl_load_errors> Read (string connectionString, DateTime dtStart)
        {
            using (var conn = new Npgsql.NpgsqlConnection (connectionString))
            {
                conn.Open ();

                foreach (var i in conn.Query<stl_load_errors> ("select filename, colname, position, err_code, err_reason from stl_load_errors where starttime >= @dtStart order by starttime desc limit 1000;", new { dtStart = dtStart.AddMinutes (-2) }))
                {
                    yield return i;
                }
            }
        }
    }

    // "select filename, lines_scanned, status from stl_load_commits where curtime >= @dtStart order by curtime desc limit 1000;"
    public class stl_load_commits
    {
        public string filename { get; set; }
        public int lines_scanned { get; set; }
        public int status { get; set; }

        public static IEnumerable<string> ReadCommitedFiles (string connectionString, DateTime dtStart)
        {
            using (var conn = new Npgsql.NpgsqlConnection (connectionString))
            {
                conn.Open ();

                foreach (var i in conn.Query<string> ("select distinct filename from stl_load_commits where status = 1 and curtime >= @dtStart order by curtime desc limit 1000;", new { dtStart = dtStart.AddMinutes (-2) }))
                {
                    yield return i;
                }
            }
        }
    }

    // "select query, slice, recordtime, lines, pct_complete, num_files, num_files_complete, current_file from STV_LOAD_STATE;"
    public class stv_load_state
    {
        public int query { get; set; }
        public int slice { get; set; }
        public DateTime recordtime { get; set; }
        public Int64 lines { get; set; }
        public int pct_complete { get; set; }
        public int num_files { get; set; }
        public int num_files_complete { get; set; }
        public string current_file { get; set; }


        public static IEnumerable<stv_load_state> Read (string connectionString)
        {
            using (var conn = new Npgsql.NpgsqlConnection (connectionString))
            {
                conn.Open ();

                foreach (var i in conn.Query<stv_load_state> ("select query, slice, recordtime, num_files, lines, pct_complete, num_files, num_files_complete, current_file from STV_LOAD_STATE;"))
                {
                    yield return i;
                }
            }
        }
    }
}
