import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Email Configuration
sender_email = "your-email@example.com"
receiver_email = "recipient@example.com"
smtp_server = "smtp.example.com"
smtp_port = 587
smtp_username = "your-email@example.com"
smtp_password = "your-email-password"

# Create email
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = "NYC Taxi Monthly Report"

# Attach PDF
with open("nyc_taxi_report.pdf", "rb") as attachment:
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", "attachment; filename=nyc_taxi_report.pdf")
    msg.attach(part)

# Send email
server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()
server.login(smtp_username, smtp_password)
server.sendmail(sender_email, receiver_email, msg.as_string())
server.quit()

print("Dashboard report has been emailed!")