using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Xml;
using TropoCSharp.Structs;
using TropoCSharp.Tropo;

namespace NotificationsPlugin
{
    public enum NotificationLanguage { pt = 0, en, es }

    public class NotificationConfig
    {
        public string EmailFrom     { get; set; }
        public string EmailLogin    { get; set; }
        public string EmailPassword { get; set; }
        public string EmailSmtp     { get; set; }
        public int    EmailPort     { get; set; }
        public bool   EmailSsl      { get; set; }
        public bool   EmailIsHtml   { get; set; }
                
        public string TropoSmsToken            { get; set; }
        public string TropoFromNumber          { get; set; }
        public string TropoCallToken           { get; set; }
        public NotificationLanguage TropoVoice { get; set; }
    }

    public class NotificationService
    {
        public static NotificationConfig Configuration { get; set; }

        public static void Configure (NotificationConfig config)
        {
            Configuration = config;
        }

        public static NotificationResult Send (NotificationTypes channel, IEnumerable<string> to, string message, string title)
        {
            try
            {
                // select operation
                switch (channel)
                {
                    case NotificationTypes.CALL:
                        return SendCall (to, message);
                    
                    case NotificationTypes.SMS:
                        return SendSms (to, message);
                    
                    case NotificationTypes.EMAIL:
                        return SendEmail (to, message, title);
                    
                    default:
                        return new NotificationResult (false, "Operação não suportada: " + channel);
                }
            }
            catch (Exception e)
            {
                return new NotificationResult (false, "NotificationResult.Send [" + channel + "] " + e.Message);
            }
        }

        private static NotificationResult SendEmail (IEnumerable<string> to, string message, string title)
        {
            // sanity check
            if (Configuration.EmailFrom.Length == 0 || !CommonUtils.ValidadeEmail (Configuration.EmailFrom))
                return new NotificationResult (false, "Parâmetro From: Email [" + Configuration.EmailFrom + "] inválido");
            // send
            var emailer = new Emailer (Configuration.EmailSmtp, Configuration.EmailPort, Configuration.EmailSsl);
            var result = emailer
                .WithLogin (Configuration.EmailLogin, Configuration.EmailPassword, Configuration.EmailFrom)
                .AddTo (to)
                .SendMail (title, message, Configuration.EmailIsHtml, false);
            return new NotificationResult (result, emailer.LastError);
        }

        private static NotificationResult SendSms (IEnumerable<string> to, string message)
        {
            // Create an XML doc to hold the response from the Tropo Session API.
            XmlDocument doc = new XmlDocument ();
            
            // send
            foreach (var num in to)
            {
                // configure parameters
                Dictionary<string, string> map = new Dictionary<string, string> ();
                map.Add ("sendToNumber", CommonUtils.ParseNumericString (num));
                map.Add ("sendFromNumber", CommonUtils.ParseNumericString (Configuration.TropoFromNumber));
                map.Add ("channel", Channel.Text);
                map.Add ("network", Network.SMS);
                map.Add ("msg", HttpUtility.UrlEncode (String.Join ("", CommonUtils.RemoveAccentuation (message))));

                // create tropo instance
                var tropo = new Tropo ();

                // Load the XML document with the return value of the CreateSession() method call.
                doc.Load (tropo.CreateSession (Configuration.TropoSmsToken, map));
            }
            // treat result
            if (String.IsNullOrEmpty (doc.InnerXml))
                throw new Exception ("Tropo operation failed");
            return new NotificationResult (true, doc.InnerXml);
        }

        private static NotificationResult SendCall (IEnumerable<string> to, string message)
        {
            // Create an XML doc to hold the response from the Tropo Session API.
            XmlDocument doc = new XmlDocument ();
            // send
            foreach (var num in to)
            {
                // configure parameters
                Dictionary<string, string> map = new Dictionary<string, string> (StringComparer.Ordinal);
                map.Add ("numberToDial", CommonUtils.ParseNumericString (num));

                switch (Configuration.TropoVoice)
                {
                    case NotificationLanguage.en:
                        map.Add ("voz", Voice.UsEnglishMale);
                        break;
                    case NotificationLanguage.es:
                        map.Add ("voz", Voice.CastilianSpanishMale);
                        break;
                    case NotificationLanguage.pt:
                    default:
                        //map.Add ("voz", Voice.PortugeseBrazilianMale); TODO: FIX PORTUGUESE BRAZILIAN LANGUAGE
                        break;
                }

                map.Add ("msg", HttpUtility.UrlEncode (String.Join("", CommonUtils.RemoveAccentuation (message)), Encoding.GetEncoding ("ISO-8859-1")));

                // create tropo instance
                var tropo = new Tropo ();

                // Load the XML document with the return value of the CreateSession() method call.
                doc.Load (tropo.CreateSession (Configuration.TropoCallToken, map));
            }
            // treat result
            if (String.IsNullOrEmpty (doc.InnerXml))
                throw new Exception ("Tropo operation failed");
            return new NotificationResult (true, doc.InnerXml);
        }       
    }

    public class NotificationResult
    {
        public bool Status { get; set; }
        public string Message { get; set; }
        
        public NotificationResult (bool result, string message = "")
        {
            Status = result;
            Message = message;
        }
    }
}
