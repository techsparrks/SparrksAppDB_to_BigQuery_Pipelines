import os

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

message = Mail(
    from_email='nospliss@gmail.com',
    to_emails='iliyana.tarpova@sparrks.de',
    subject='Sending with Twilio SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong>')
try:
    # apikey = os.environ.get('SENDGRID_API_KEY', '')
    sg = SendGridAPIClient(api_key='')
    response = sg.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)
except Exception as e:
    print(e.message)
