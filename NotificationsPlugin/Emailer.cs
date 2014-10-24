using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace NotificationsPlugin
{
    public struct EmailRecepient
    {
        public string DisplayName;
        public string EmailAddress;

        public EmailRecepient (string emailAddress, string displayName)
        {
            DisplayName = displayName;
            EmailAddress = emailAddress;
        }
    }

    public class Emailer : IDisposable
    {
        public enum SmtpServerType
        {
            Normal,
            SSL,
            TSL,
            TSLSecure,
            Gmail,
            AWS
        }

        #region **  Private members     **

        private SmtpClient m_clienteSmtp;
        private MailMessage m_mailMessage;

        #endregion

        #region IDisposable interface implementation

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose ()
        {
            Close ();
            // This object will be cleaned up by the Dispose method.
            // Therefore, you should call GC.SupressFinalize to
            // take this object off the finalization queue
            // and prevent finalization code for this object
            // from executing a second time.
            GC.SuppressFinalize (this);
        }

        // Use C# destructor syntax for finalization code.
        // This destructor will run only if the Dispose method
        // does not get called.
        // It gives your base class the opportunity to finalize.
        // Do not provide destructors in types derived from this class.
        ~Emailer ()
        {
            Close ();
        }

        public void Close ()
        {
            try
            {
                if (m_mailMessage != null)
                {
                    m_mailMessage.Dispose ();
                }
                m_mailMessage = null;
                if (m_clienteSmtp != null)
                    m_clienteSmtp.Dispose ();
                m_clienteSmtp = null;
            }
            catch {}
        }

        #endregion

        #region **  Properties  **

        public int TimeOut
        {
            get { return m_clienteSmtp.Timeout; }
            set { m_clienteSmtp.Timeout = value; }
        }

        public string SMTPServer
        {
            get { return m_clienteSmtp.Host; }
            set { m_clienteSmtp.Host = value; }
        }

        public int PortSMTP
        {
            get { return m_clienteSmtp.Port; }
            set { m_clienteSmtp.Port = value; }
        }

        public string MessageBody
        {
            get { return m_mailMessage.Body; }
            set { m_mailMessage.Body = value; }
        }

        public string Subject
        {
            get { return m_mailMessage.Subject; }
            set { m_mailMessage.Subject = value; }
        }

        public bool IsHTMLBody
        {
            get { return m_mailMessage.IsBodyHtml; }
            set { m_mailMessage.IsBodyHtml = value; }
        }

        public bool IsSSLSecure
        {
            get { return m_clienteSmtp.EnableSsl; }
            set { m_clienteSmtp.EnableSsl = value; }
        }

        public MailMessage MessageInstance
        {
            get { return m_mailMessage; }
        }

        public string LastError { get; private set; }

        #endregion

        #region **  Constructor     **

        /// <summary>
        /// Initializes a new instance of the <see cref="Emailer" /> class.
        /// </summary>
        private Emailer ()
        {
            m_mailMessage = new MailMessage () ;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Emailer" /> class.
        /// </summary>
        /// <param name="SmtpServer">The SMTP server.</param>
        /// <param name="SmtpPort">The SMTP port.</param>
        /// <param name="isSSLSecure">The is SSL secure.</param>
        public Emailer (string SmtpServer, int SmtpPort, bool isSSLSecure) : this ()
        {
            m_clienteSmtp = new SmtpClient (SmtpServer, SmtpPort) ;
            m_clienteSmtp.EnableSsl = isSSLSecure;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Emailer" /> class.
        /// </summary>
        /// <param name="SmtpServer">The SMTP server.</param>
        /// <param name="svrType">Kind of the smtp server.</param>
        public Emailer (string SmtpServer, SmtpServerType svrType) : this ()
        {
            switch (svrType)
            {
                case SmtpServerType.SSL:
                    m_clienteSmtp = new SmtpClient (SmtpServer, 465) ;
                    m_clienteSmtp.EnableSsl = true;
                    break;
                case SmtpServerType.TSL:
                    m_clienteSmtp = new SmtpClient (SmtpServer, 587) ;
                    break;
                case SmtpServerType.TSLSecure:
                case SmtpServerType.AWS:
                case SmtpServerType.Gmail:
                    m_clienteSmtp = new SmtpClient (SmtpServer, 587) ;
                    m_clienteSmtp.EnableSsl = true;
                    break;
                case SmtpServerType.Normal:
                default:
                    m_clienteSmtp = new SmtpClient (SmtpServer, 25) ;
                    break;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Emailer" /> class.
        /// </summary>
        /// <param name="SmtpServer">The SMTP server.</param>
        /// <param name="title">The title.</param>
        /// <param name="svrType">Kind of the smtp server.</param>
        public Emailer (string SmtpServer, string title, SmtpServerType svrType = SmtpServerType.Normal) : this (SmtpServer, svrType)
        {
            m_mailMessage.Subject = title;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Emailer" /> class.
        /// </summary>
        /// <param name="SmtpServer">The SMTP server.</param>
        /// <param name="title">The title.</param>
        /// <param name="recipientAddress">The recipient address.</param>
        /// <param name="recipientName">Name of the recipient.</param>
        /// <param name="destinataryList">The destinatary list.</param>
        /// <param name="svrType">Kind of the smtp server.</param>
        public Emailer (string SmtpServer, string title, string recipientAddress, string recipientName, List<EmailRecepient> destinataryList,
                        SmtpServerType svrType = SmtpServerType.Normal)
        : this (SmtpServer, title, svrType)
        {
            m_mailMessage.From = new MailAddress (recipientAddress, recipientName) ;
            AddTo (destinataryList);
        }

        /// <summary>
        /// Creates the gmail.
        /// </summary>
        /// <returns></returns>
        public static Emailer CreateGmail ()
        {
            return new Emailer ("smtp.gmail.com", SmtpServerType.Gmail);
        }

        #endregion

        #region **  Public  **

        /// <summary>
        /// Configures the server login information for the email sender in case of
        /// not using the Default Credentials.
        /// It is important when using services like gmail.
        /// </summary>
        /// <param name="userName">Name of the user.</param>
        /// <param name="password">The password.</param>
        /// <param name="fromAdd">From field with recepient email address.</param>
        public void LoginInformation (string userName, string password, string fromAdd = "")
        {
            if (m_clienteSmtp == null)
                throw new NullReferenceException ("ClientSMTP is not defined. Emailer object must be created first.");

            m_clienteSmtp.UseDefaultCredentials = false;
            if (String.IsNullOrEmpty (fromAdd))
                fromAdd = userName;
            NetworkCredential smtpUserInfo = new NetworkCredential (userName, password) ;
            if (CommonUtils.ValidadeEmail (fromAdd))
                m_mailMessage.From = new MailAddress (fromAdd) ;            
            m_clienteSmtp.Credentials = smtpUserInfo;
        }

        public Emailer WithLogin (string userName, string password, string fromAdd = "")
        {
            LoginInformation (userName, password, fromAdd);
            return this;
        }

        public Emailer AddTo (EmailRecepient email)
        {
            if (CommonUtils.ValidadeEmail (email.EmailAddress))
            { // adds each address to the To field of the email
                m_mailMessage.To.Add (new MailAddress (email.EmailAddress, email.DisplayName));
            }
            return this;
        }

        public Emailer AddTo (string email, string name = "")
        {
            if (String.IsNullOrEmpty (name))
                name = email;
            if (CommonUtils.ValidadeEmail (email))
            { // adds each address to the To field of the email
                m_mailMessage.To.Add (new MailAddress (email, name));
            }
            return this;
        }

        public Emailer AddTo (IEnumerable<string> emailList)
        {
            foreach (var s in emailList)
            {
                AddTo (s);
            }
            return this;
        }

        public Emailer AddTo (IEnumerable<EmailRecepient> emailList)
        {
            foreach (var s in emailList)
            {
                AddTo (s);
            }
            return this;
        }

        public Emailer AddCC (EmailRecepient email)
        {
            if (CommonUtils.ValidadeEmail (email.EmailAddress))
            { // adds each address to the To field of the email
                m_mailMessage.CC.Add (new MailAddress (email.EmailAddress, email.DisplayName));
            }
            return this;
        }

        public Emailer AddCC (string email, string name = "")
        {
            if (String.IsNullOrEmpty (name))
                name = email;
            if (CommonUtils.ValidadeEmail (email))
            { // adds each address to the To field of the email
                m_mailMessage.CC.Add (new MailAddress (email, name));
            }
            return this;
        }

        public Emailer AddCC (IEnumerable<string> emailList)
        {
            foreach (var s in emailList)
            {
                AddCC (s);
            }
            return this;
        }

        public Emailer AddCC (IEnumerable<EmailRecepient> emailList)
        {
            foreach (var s in emailList)
            {
                AddCC (s);
            }
            return this;
        }

        public Emailer AddBcc (EmailRecepient email)
        {
            if (CommonUtils.ValidadeEmail (email.EmailAddress))
            { // adds each address to the To field of the email
                m_mailMessage.Bcc.Add (new MailAddress (email.EmailAddress, email.DisplayName));
            }
            return this;
        }

        public Emailer AddBcc (string email, string name = "")
        {
            if (String.IsNullOrEmpty (name))
                name = email;
            if (CommonUtils.ValidadeEmail (email))
            { // adds each address to the To field of the email
                m_mailMessage.Bcc.Add (new MailAddress (email, name));
            }
            return this;
        }

        public Emailer AddBcc (IEnumerable<string> emailList)
        {
            foreach (var s in emailList)
            {
                AddBcc (s);
            }
            return this;
        }

        public Emailer AddBcc (IEnumerable<EmailRecepient> emailList)
        {
            foreach (var s in emailList)
            {
                AddBcc (s);
            }
            return this;
        }

        /// <summary>
        /// Sends the email.
        /// </summary>
        /// <param name="logError">The log error.</param>
        /// <returns></returns>
        public bool SendMail (bool throwOnError)
        {
            LastError = String.Empty;
            try
            {
                using (m_clienteSmtp)
                    m_clienteSmtp.Send (m_mailMessage);
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                if (throwOnError)
                    throw ex;
                return false;
            }
            finally
            {
                Close ();
            }
            m_clienteSmtp = null;
            return true;
        }

        /// <summary>
        /// Sends the mail.
        /// </summary>
        /// <param name="emailTitle">The email title.</param>
        /// <param name="msg">The MSG.</param>
        /// <param name="isHtml">The is HTML.</param>
        /// <param name="logError">The log error.</param>
        /// <returns></returns>
        public bool SendMail (string emailTitle, string msg, bool isHtml, bool throwOnError)
        {
            WithMessage (emailTitle, msg, isHtml);
            return SendMail (throwOnError);
        }

        /// <summary>
        /// Adds the file as an attachment.
        /// </summary>
        /// <param name="fileName">Full file path.</param>
        /// <returns></returns>
        public Emailer AddFileAttachment (string fileName)
        {
            m_mailMessage.Attachments.Add (new Attachment (fileName));
            return this;
        }

        /// <summary>
        /// Adds the file as an attachment.
        /// </summary>
        /// <param name="item">The Attachment object.</param>
        /// <returns></returns>
        public Emailer AddFileAttachment (Attachment item)
        {
            m_mailMessage.Attachments.Add (item);
            return this;
        }

        /// <summary>
        /// Adds the file as an attachment.
        /// </summary>
        /// <param name="contentStream">The content stream.</param>
        /// <param name="fileName">Name of the file.</param>
        /// <returns></returns>
        public Emailer AddFileAttachment (Stream contentStream, string fileName)
        {
            m_mailMessage.Attachments.Add (new Attachment (contentStream, fileName));
            return this;
        }

        /// <summary>
        /// Set the email message body.
        /// </summary>
        /// <param name="emailTitle">The email title.</param>
        /// <param name="msg">The message.</param>
        /// <param name="isHtml">If the message is html encoded.</param>
        /// <param name="convertLineBreaks">The convert all line breaks to html line break.</param>
        public Emailer WithMessage (string emailTitle, string msg, bool isHtml = true, bool convertLineBreaks = false)
        {
            this.Subject = emailTitle;
            WithMessage (msg, isHtml, convertLineBreaks);
            return this;
        }

        /// <summary>
        /// Set the email message body.
        /// </summary>
        /// <param name="msg">The message.</param>
        /// <param name="isHtml">If the message is html encoded.</param>
        /// <param name="convertLineBreaks">The convert all line breaks to html line break.</param>
        /// <returns></returns>
        public Emailer WithMessage (string msg, bool isHtml = true, bool convertLineBreaks = false)
        {
            MessageBody = msg;
            if (isHtml && convertLineBreaks)
                MessageBody = msg.Replace (Environment.NewLine, "<br />");
            IsHTMLBody = isHtml;
            return this;
        }

        #endregion

    }
}
