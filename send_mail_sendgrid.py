import os

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

message = Mail(
    from_email='nospliss@gmail.com',
    to_emails='iliyana.tarpova@sparrks.de',
    subject='Sending with Twilio SendGrid is Fun',
    html_content='<strong>and easy to do anywhere, even with Python</strong>')
try:
    # apikey = os.environ.get('SENDGRID_API_KEY', 'yPAAYsblQ5q_ZehEE1SSrw')
    sg = SendGridAPIClient(api_key='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')
    response = sg.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)
except Exception as e:
    print(e.message)
