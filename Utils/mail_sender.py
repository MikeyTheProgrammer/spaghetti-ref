# this file is not in use.
# this file contains functions such as attach mail and handles the mail option for the output options.

import os
import mimetypes
import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase

USER_NAME = 'redash'
USER_PASSWORD = 'HelloWorld8200Pakar!'

#This function attaches a file to an email message
def attach_file(attached_file_path, message):
    ctype, guessed_encoding = mimetypes.guess_type(attached_file_path)
    if ctype is None or guessed_encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    if maintype == "text":
        fp = open(attached_file_path)
        attachment = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "image":
        fp = open(attached_file_path, "rb")
        attachment = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "audio":
        fp = open(attached_file_path, "rb")
        attachment = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(attached_file_path, "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
    attached_file_name = os.path.basename(attached_file_path)
    attachment.add_header("Content-Disposition", "attachment", filename=attached_file_name)
    message.attach(attachment)

#This function sends an email.
def send_mail(sender, receiver, subject, attached_file_path=None):
    message = MIMEMultipart('multipart')
    message['From'] = sender
    message['To'] = receiver
    message['Subject'] = subject

    if attached_file_path is not None:
        attach_file(attached_file_path, message)

    #SMTPserver = '192.168.239.36'
    SMTPserver = '192.168.239.98'
    smtp_port_number = 25

    server = smtplib.SMTP(SMTPserver, port=smtp_port_number, timeout=3000)
    server.login(USER_NAME, USER_PASSWORD)
    server.sendmail(sender, receiver, message.as_string())
    server.quit()
