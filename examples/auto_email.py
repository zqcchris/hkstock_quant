"""
如果想使用亚马逊云服务器的邮件发送功能，需要服务器开通25端口；
但是目前亚马逊云服务器的25号端口需要申请后才可以开放，邮件通知功能暂时无法使用。
"""
import os
import sys
sys.path.append(os.getcwd())
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import parseaddr, formataddr


def _format_addr(s):
    name, addr = parseaddr(s)
    # 将邮件的name转换成utf-8格式，addr如果是unicode，则转换utf-8输出，否则直接输出addr
    return formataddr((Header(name, 'utf-8').encode(), addr))


def send_attachment(file, receivers, subject, body):
    mail_host = "smtp.163.com"  # 设置服务器
    mail_user = "bsk_quant@163.com"  # 用户名
    mail_pass = "FAPGYGETAFYHFOIM"  # 口令

    sender = 'bsk_quant@163.com'
    receiver = ','.join(receivers)

    message = MIMEMultipart()
    message.attach(MIMEText(body, 'plain', 'utf-8'))
    message['From'] = _format_addr(u'币胜客量化 <%s>' % sender)
    message['To'] = receiver
    message['Subject'] = Header(subject, 'utf-8')

    att1 = MIMEText(open(file, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # filename不能有中文
    att1["Content-Disposition"] = 'attachment; filename="%s"' % (file.split(os.sep)[-1])
    message.attach(att1)

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException as e:
        print("Error: 无法发送邮件")
        print(e)


def send_notification(receivers, subject, body):
    mail_host = "smtp.163.com"  # 设置服务器
    mail_user = "bsk_quant@163.com"  # 用户名
    mail_pass = "FAPGYGETAFYHFOIM"  # 口令

    sender = 'bsk_quant@163.com'
    receiver = ','.join(receivers)

    message = MIMEText(body, 'plain', 'utf-8')
    message['From'] = _format_addr(u'币胜客量化 <%s>' % sender)
    message['To'] = receiver
    message['Subject'] = Header(subject, 'utf-8')

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException as e:
        print("Error: 无法发送邮件")
        print(e)


subject = '币胜客量化微信公众号更新啦，欢迎关注'
body = '尊敬的客户，币胜客量化公众号已经做了升级，欢迎扫描附件二维码关注'
# receivers = ['763118740@qq.com', "zqcchris@163.com"]
receivers = ["zqcchris@163.com"]
file = "/Users/qingchenzhao/Desktop/aa.jpg"
file_name = file.split(os.sep)[-1]
print(file_name)
send_notification(receivers=receivers, subject=subject, body=body)
send_attachment(file=file, receivers=receivers, subject=subject, body=body)
