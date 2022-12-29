import os, copy
import smtplib               # SMTP 라이브러리
from string import Template  # 문자열 템플릿 모듈
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from email.mime.image     import MIMEImage
import datetime
import sys
sys.path.append('/mnt/md0/routine/account_info/')
import account_info

id, pw = account_info.jinyoungbot0308()
curt_day = (datetime.datetime.now() - datetime.timedelta(days=1)).date()

class EmailHTMLImageContent:
    """e메일에 담길 이미지가 포함된 컨텐츠"""

    def __init__(self, str_subject, str_image_file_name, str_cid_name, template, template_params):
        """이미지파일(str_image_file_name), 컨텐츠ID(str_cid_name)사용된 string template과 딕셔너리형 template_params받아 MIME 메시지를 만든다"""
        assert isinstance(template, Template)
        assert isinstance(template_params, dict)
        self.msg = MIMEMultipart()

        # e메일 제목을 설정한다
        self.msg['Subject'] = str_subject  # e메일 제목을 설정한다

        # e메일 본문을 설정한다
        str_msg = template.safe_substitute(**template_params)  # ${변수} 치환하며 문자열 만든다
        mime_msg = MIMEText(str_msg, 'html')  # MIME HTML 문자열을 만든다
        self.msg.attach(mime_msg)

        # e메일 본문에 이미지를 임베딩한다
        assert template.template.find("cid:" + str_cid_name) >= 0, 'template must have cid for embedded image.'
        assert os.path.isfile(str_image_file_name), 'image file does not exist.'
        with open(str_image_file_name, 'rb') as img_file:
            mime_img = MIMEImage(img_file.read())
            mime_img.add_header('Content-ID', '<' + str_cid_name + '>')
        self.msg.attach(mime_img)
        
        assert template.template.find("cid:my_image2") >= 0, 'template must have cid for embedded image.'
        assert os.path.isfile("/mnt/md0/routine/daily_collection_mail/routine_image/Table_description.png"), 'image file does not exist'
        with open('/mnt/md0/routine/daily_collection_mail/routine_image/Table_description.png', 'rb') as img_file2:
            mime_img = MIMEImage(img_file2.read())
            mime_img.add_header('Content-ID', '<' + 'my_image2' + '>')
        self.msg.attach(mime_img)


    def get_message(self, str_from_email_addr, str_to_eamil_addrs):
        """발신자, 수신자리스트를 이용하여 보낼메시지를 만든다 """
        mm = copy.deepcopy(self.msg)
        mm['From'] = str_from_email_addr  # 발신자
        mm['To'] = ",".join(str_to_eamil_addrs)  # 수신자리스트
        return mm

class EmailSender:
    """e메일 발송자"""

    def __init__(self, str_host, num_port=25):
        """호스트와 포트번호로 SMTP로 연결한다 """
        self.str_host = str_host
        self.num_port = num_port
        self.ss = smtplib.SMTP(host=str_host, port=num_port)
        # SMTP인증이 필요하면 아래 주석을 해제
        self.ss.starttls() # TLS(Transport Layer Security) 시작
        self.ss.login(id, pw) # 메일서버에 연결한 계정과 비밀번호

    def send_message(self, emailContent, str_from_email_addr, str_to_eamil_addrs):
        """e메일을 발송한다 """
        cc = emailContent.get_message(str_from_email_addr, str_to_eamil_addrs)
        self.ss.send_message(cc, from_addr=str_from_email_addr, to_addrs=str_to_eamil_addrs)
        del cc

emailSender = EmailSender("smtp.gmail.com")

str_subject = f'{curt_day} Daily data collection rate check'
template = Template("""<html>
                            <head></head>
                            <body> 
                                <img src="cid:my_image1"><br>
                                <img src="cid:my_image2"><br>
                            </body>
                        </html>""")
template_params       = {'NAME':'Signal house'}
str_image_file_name   = "/mnt/md0/routine/daily_collection_mail/routine_image/Daily_OP.png"
str_cid_name          = 'my_image1'
emailHTMLImageContent = EmailHTMLImageContent(str_subject, str_image_file_name, str_cid_name, template, template_params)

str_from_email_addr = "jinyoungbot0308@gmail.com" # 발신자
str_to_eamil_addrs  = ["jinyoungkim0308@gmail.com", "zephyrus02@gmail.com", "dien.eaststar@gmail.com", "bapor.km.kang@gmail.com", "wjdgns03125@gmail.com", "hskim.bme@gmail.com", "shkimans@gmail.com "] # 수신자리스트
#str_to_eamil_addrs = ["jinyoungkim0308@gmail.com"]
emailSender.send_message(emailHTMLImageContent, str_from_email_addr, str_to_eamil_addrs)







