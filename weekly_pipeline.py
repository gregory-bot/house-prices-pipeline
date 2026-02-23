import schedule
import time
import subprocess
import smtplib
from email.message import EmailMessage

def send_email():
    msg = EmailMessage()
    msg['Subject'] = 'Nairobi Property Data Pipeline Update'
    msg['From'] = 'kipngenogregory@gmail.com'
    msg['To'] = 'kipngenogregory@gmail.com'
    msg.set_content('Weekly property data scraping, cleaning, and upload to PostgreSQL completed successfully.')

    # Gmail SMTP settings
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_user = 'kipngenogregory@gmail.com'
    smtp_password = 'YOUR_APP_PASSWORD'  # Use an app password, not your main Gmail password

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        print('Email sent successfully.')

def run_pipeline():
    # Run the full pipeline
    subprocess.run(['C:/Users/kipng/nairobi_property_pricing/.venv/Scripts/python.exe', 'scrape_listings.py'])
    subprocess.run(['C:/Users/kipng/nairobi_property_pricing/.venv/Scripts/python.exe', 'clean_properties.py'])
    subprocess.run(['C:/Users/kipng/nairobi_property_pricing/.venv/Scripts/python.exe', 'prepare_properties.py'])
    send_email()

# Schedule to run every Monday at 8:00 AM
schedule.every().monday.at('08:00').do(run_pipeline)

print('Scheduler started. Waiting for next run...')
while True:
    schedule.run_pending()
    time.sleep(60)
